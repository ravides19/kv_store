defmodule KVStore.Storage.Cache do
  @moduledoc """
  Read-ahead cache for frequently accessed data.

  This module implements:
  - LRU (Least Recently Used) cache for hot values
  - Read-ahead prediction for sequential access patterns
  - Memory-bounded cache with configurable size limits
  - Statistics and monitoring for cache performance
  """

  use GenServer
  require Logger

  # Default cache configuration
  @default_max_entries 1000
  @default_max_memory_mb 100
  # 5 minutes
  @default_ttl_seconds 300

  defstruct [
    :max_entries,
    :max_memory_bytes,
    :ttl_seconds,
    # ETS table for O(1) lookups
    :cache_map,
    # ETS table for LRU ordering
    :lru_list,
    # Cache statistics
    :stats,
    # Last cleanup timestamp
    :last_cleanup
  ]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Client API

  @doc """
  Get a value from cache.
  """
  def get(key) do
    GenServer.call(__MODULE__, {:get, key})
  end

  @doc """
  Put a value in cache.
  """
  def put(key, value) do
    GenServer.cast(__MODULE__, {:put, key, value})
  end

  @doc """
  Remove a value from cache.
  """
  def delete(key) do
    GenServer.cast(__MODULE__, {:delete, key})
  end

  @doc """
  Get cache statistics.
  """
  def stats do
    GenServer.call(__MODULE__, :stats)
  end

  @doc """
  Clear the entire cache.
  """
  def clear do
    GenServer.cast(__MODULE__, :clear)
  end

  @doc """
  Predict and cache values for sequential access patterns.
  """
  def read_ahead(current_key, pattern_fn) do
    GenServer.cast(__MODULE__, {:read_ahead, current_key, pattern_fn})
  end

  # Server callbacks

  @impl true
  def init(opts) do
    max_entries = Keyword.get(opts, :max_entries, @default_max_entries)
    max_memory_mb = Keyword.get(opts, :max_memory_mb, @default_max_memory_mb)
    ttl_seconds = Keyword.get(opts, :ttl_seconds, @default_ttl_seconds)

    # Create ETS tables
    cache_map = :ets.new(:kv_cache_map, [:set, :public])
    lru_list = :ets.new(:kv_cache_lru, [:set, :public])

    # Initialize statistics
    stats = %{
      hits: 0,
      misses: 0,
      puts: 0,
      deletes: 0,
      evictions: 0,
      read_ahead_hits: 0,
      total_memory_bytes: 0
    }

    state = %__MODULE__{
      max_entries: max_entries,
      max_memory_bytes: max_memory_mb * 1024 * 1024,
      ttl_seconds: ttl_seconds,
      cache_map: cache_map,
      lru_list: lru_list,
      stats: stats,
      last_cleanup: :os.system_time(:second)
    }

    Logger.info("Cache started with max_entries=#{max_entries}, max_memory=#{max_memory_mb}MB")
    {:ok, state}
  end

  @impl true
  def terminate(_reason, state) do
    :ets.delete(state.cache_map)
    :ets.delete(state.lru_list)
    :ok
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    case get_cached_value(key, state) do
      {:ok, value} ->
        new_stats = %{state.stats | hits: state.stats.hits + 1}
        {:reply, {:ok, value}, %{state | stats: new_stats}}

      {:error, :not_found} ->
        new_stats = %{state.stats | misses: state.stats.misses + 1}
        {:reply, {:error, :not_found}, %{state | stats: new_stats}}
    end
  end

  @impl true
  def handle_call(:stats, _from, state) do
    current_stats = calculate_current_stats(state)
    {:reply, current_stats, state}
  end

  @impl true
  def handle_cast({:put, key, value}, state) do
    new_state = put_cached_value(key, value, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:delete, key}, state) do
    new_state = delete_cached_value(key, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast(:clear, state) do
    :ets.delete_all_objects(state.cache_map)
    :ets.delete_all_objects(state.lru_list)

    new_stats = %{state.stats | total_memory_bytes: 0}
    {:noreply, %{state | stats: new_stats}}
  end

  @impl true
  def handle_cast({:read_ahead, current_key, pattern_fn}, state) do
    # Predict next keys based on pattern
    predicted_keys = pattern_fn.(current_key)

    # Fetch and cache predicted values
    new_state =
      Enum.reduce(predicted_keys, state, fn predicted_key, acc_state ->
        case fetch_value_for_read_ahead(predicted_key) do
          {:ok, value} ->
            put_cached_value(predicted_key, value, acc_state)

          {:error, _reason} ->
            acc_state
        end
      end)

    {:noreply, new_state}
  end

  @impl true
  def handle_info(:cleanup_expired, state) do
    new_state = cleanup_expired_entries(state)

    # Schedule next cleanup
    # Every minute
    Process.send_after(self(), :cleanup_expired, 60_000)

    {:noreply, new_state}
  end

  # Private functions

  defp get_cached_value(key, state) do
    case :ets.lookup(state.cache_map, key) do
      [{^key, value, timestamp, _size}] ->
        # Check if entry is expired
        if is_expired(timestamp, state.ttl_seconds) do
          # Remove expired entry
          delete_cached_value(key, state)
          {:error, :not_found}
        else
          # Update LRU order
          update_lru_order(key, timestamp, state)
          {:ok, value}
        end

      [] ->
        {:error, :not_found}
    end
  end

  defp put_cached_value(key, value, state) do
    value_size = estimate_value_size(value)
    timestamp = :os.system_time(:second)

    # Check if we need to evict entries
    new_state = ensure_cache_space(key, value_size, state)

    # Insert new entry
    :ets.insert(new_state.cache_map, {key, value, timestamp, value_size})
    :ets.insert(new_state.lru_list, {timestamp, key})

    new_stats = %{
      new_state.stats
      | puts: new_state.stats.puts + 1,
        total_memory_bytes: new_state.stats.total_memory_bytes + value_size
    }

    %{new_state | stats: new_stats}
  end

  defp delete_cached_value(key, state) do
    case :ets.lookup(state.cache_map, key) do
      [{^key, _value, timestamp, size}] ->
        :ets.delete(state.cache_map, key)
        :ets.delete(state.lru_list, {timestamp, key})

        new_stats = %{
          state.stats
          | deletes: state.stats.deletes + 1,
            total_memory_bytes: state.stats.total_memory_bytes - size
        }

        %{state | stats: new_stats}

      [] ->
        state
    end
  end

  defp ensure_cache_space(_new_key, new_value_size, state) do
    current_entries = :ets.info(state.cache_map, :size)
    current_memory = state.stats.total_memory_bytes

    cond do
      # Check entry count limit
      current_entries >= state.max_entries ->
        evict_lru_entry(state)

      # Check memory limit
      current_memory + new_value_size > state.max_memory_bytes ->
        evict_until_space_available(new_value_size, state)

      true ->
        state
    end
  end

  defp evict_lru_entry(state) do
    # Find the oldest entry by scanning all entries
    case :ets.tab2list(state.lru_list) do
      [] ->
        state

      entries ->
        # Find the entry with the smallest timestamp
        {oldest_timestamp, oldest_key} =
          Enum.min_by(entries, fn {timestamp, _key} -> timestamp end)

        case :ets.lookup(state.cache_map, oldest_key) do
          [{^oldest_key, _value, ^oldest_timestamp, size}] ->
            :ets.delete(state.cache_map, oldest_key)
            :ets.delete(state.lru_list, {oldest_timestamp, oldest_key})

            new_stats = %{
              state.stats
              | evictions: state.stats.evictions + 1,
                total_memory_bytes: state.stats.total_memory_bytes - size
            }

            %{state | stats: new_stats}

          _ ->
            # Inconsistent state, clean up
            :ets.delete(state.lru_list, {oldest_timestamp, oldest_key})
            evict_lru_entry(state)
        end
    end
  end

  defp evict_until_space_available(required_space, state) do
    if state.stats.total_memory_bytes + required_space <= state.max_memory_bytes do
      state
    else
      new_state = evict_lru_entry(state)
      evict_until_space_available(required_space, new_state)
    end
  end

  defp update_lru_order(key, old_timestamp, state) do
    new_timestamp = :os.system_time(:second)

    # Remove old entry
    :ets.delete(state.lru_list, {old_timestamp, key})

    # Insert new entry with updated timestamp
    :ets.insert(state.lru_list, {new_timestamp, key})

    # Update cache map with new timestamp
    case :ets.lookup(state.cache_map, key) do
      [{^key, value, _old_timestamp, size}] ->
        :ets.insert(state.cache_map, {key, value, new_timestamp, size})

      _ ->
        :ok
    end
  end

  defp is_expired(timestamp, ttl_seconds) do
    current_time = :os.system_time(:second)
    current_time - timestamp > ttl_seconds
  end

  defp estimate_value_size(value) when is_binary(value) do
    byte_size(value)
  end

  defp estimate_value_size(value) when is_list(value) do
    # Rough estimation for lists
    length(value) * 8
  end

  defp estimate_value_size(value) when is_map(value) do
    # Rough estimation for maps
    map_size(value) * 16
  end

  defp estimate_value_size(_value) do
    # Default estimation for other types
    64
  end

  defp fetch_value_for_read_ahead(key) do
    # This would typically fetch from the storage engine
    # For now, we'll return a placeholder
    case KVStore.Storage.Engine.get(key) do
      {:ok, value} -> {:ok, value}
      {:error, _reason} -> {:error, :not_found}
    end
  end

  defp cleanup_expired_entries(state) do
    current_time = :os.system_time(:second)

    # Scan LRU list and remove expired entries
    cleanup_expired_entries_recursive(current_time, state.ttl_seconds, state)
  end

  defp cleanup_expired_entries_recursive(current_time, ttl_seconds, state) do
    case :ets.first(state.lru_list) do
      :"$end_of_table" ->
        state

      {timestamp, key} ->
        if current_time - timestamp > ttl_seconds do
          # Entry is expired, remove it
          new_state = delete_cached_value(key, state)
          cleanup_expired_entries_recursive(current_time, ttl_seconds, new_state)
        else
          # No more expired entries
          state
        end
    end
  end

  defp calculate_current_stats(state) do
    current_entries = :ets.info(state.cache_map, :size)
    hit_rate = calculate_hit_rate(state.stats)

    Map.merge(state.stats, %{
      current_entries: current_entries,
      hit_rate: hit_rate,
      memory_usage_mb: state.stats.total_memory_bytes / (1024 * 1024)
    })
  end

  defp calculate_hit_rate(%{hits: hits, misses: misses}) do
    total = hits + misses

    if total > 0 do
      hits / total
    else
      0.0
    end
  end
end
