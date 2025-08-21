defmodule KVStore.Storage.Engine do
  @moduledoc """
  Main storage engine GenServer.

  Handles:
  - Active file management
  - ETS index operations
  - Key-value operations (put, get, delete, range, batch_put)
  - Segment rotation
  """

  use GenServer
  require Logger

  # Configuration defaults
  @default_data_dir "data"
  # 100MB
  @default_segment_max_bytes 100 * 1024 * 1024
  @default_sync_on_put false

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Client API

  @doc """
  Put a key-value pair into the store.
  """
  def put(key, value) do
    GenServer.call(__MODULE__, {:put, key, value})
  end

  @doc """
  Get a value by key.
  """
  def get(key) do
    GenServer.call(__MODULE__, {:get, key})
  end

  @doc """
  Delete a key from the store.
  """
  def delete(key) do
    GenServer.call(__MODULE__, {:delete, key})
  end

  @doc """
  Get a range of keys.
  """
  def range(start_key, end_key) do
    GenServer.call(__MODULE__, {:range, start_key, end_key})
  end

  @doc """
  Batch put multiple key-value pairs.
  """
  def batch_put(kv_pairs) do
    GenServer.call(__MODULE__, {:batch_put, kv_pairs})
  end

  @doc """
  Get storage engine status.
  """
  def status do
    GenServer.call(__MODULE__, :status)
  end

  # Server callbacks

  @impl true
  def init(opts) do
    data_dir = Keyword.get(opts, :data_dir, @default_data_dir)
    segment_max_bytes = Keyword.get(opts, :segment_max_bytes, @default_segment_max_bytes)
    sync_on_put = Keyword.get(opts, :sync_on_put, @default_sync_on_put)

    # Ensure data directory exists
    File.mkdir_p!(data_dir)

    # Initialize ETS tables
    keydir = :ets.new(:keydir, [:set, :public, read_concurrency: true, write_concurrency: true])

    key_set =
      :ets.new(:key_set, [:ordered_set, :public, read_concurrency: true, write_concurrency: true])

    # Initialize state
    state = %{
      data_dir: data_dir,
      segment_max_bytes: segment_max_bytes,
      sync_on_put: sync_on_put,
      keydir: keydir,
      key_set: key_set,
      active_segment_id: 1,
      active_file: nil,
      active_offset: 0
    }

    # Open or create active segment
    state = open_active_segment(state)

    Logger.info("KVStore storage engine started with data_dir=#{data_dir}")
    {:ok, state}
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    # Create record data
    record_data = KVStore.Storage.Record.create(key, value)

    # Check if we need to rotate the segment
    state = maybe_rotate_segment(state)

    # Write record to active segment
    case KVStore.Storage.Segment.write_record(state.active_file, record_data, state.active_offset) do
      {:ok, record_offset, new_offset} ->
        # Update ETS index
        :ets.insert(
          state.keydir,
          {key, state.active_segment_id, record_offset, KVStore.Storage.Record.size(key, value),
           :os.system_time(:millisecond), false}
        )

        :ets.insert(state.key_set, {key, true})

        # Sync if configured
        if state.sync_on_put do
          KVStore.Storage.Segment.sync(state.active_file)
        end

        {:reply, {:ok, record_offset}, %{state | active_offset: new_offset}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    case :ets.lookup(state.keydir, key) do
      [] ->
        {:reply, {:error, :not_found}, state}

      [{^key, segment_id, offset, _size, _timestamp, is_tombstone}] ->
        if is_tombstone do
          {:reply, {:error, :not_found}, state}
        else
          # Read from the appropriate segment
          case read_from_segment(segment_id, offset, state) do
            {:ok, value} ->
              {:reply, {:ok, value}, state}

            {:error, reason} ->
              {:reply, {:error, reason}, state}
          end
        end
    end
  end

  @impl true
  def handle_call({:delete, key}, _from, state) do
    # Create tombstone record
    record_data = KVStore.Storage.Record.create_tombstone(key)

    # Check if we need to rotate the segment
    state = maybe_rotate_segment(state)

    # Write tombstone to active segment
    case KVStore.Storage.Segment.write_record(state.active_file, record_data, state.active_offset) do
      {:ok, record_offset, new_offset} ->
        # Update ETS index - mark as tombstone
        :ets.insert(
          state.keydir,
          {key, state.active_segment_id, record_offset, KVStore.Storage.Record.size(key, ""),
           :os.system_time(:millisecond), true}
        )

        :ets.delete(state.key_set, key)

        # Sync if configured
        if state.sync_on_put do
          KVStore.Storage.Segment.sync(state.active_file)
        end

        {:reply, {:ok, record_offset}, %{state | active_offset: new_offset}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:range, start_key, end_key}, _from, state) do
    # Use the ordered set to get keys in range
    keys_in_range = get_keys_in_range(state.key_set, start_key, end_key)

    # Read values for keys in range
    results =
      Enum.reduce_while(keys_in_range, [], fn key, acc ->
        case :ets.lookup(state.keydir, key) do
          [] ->
            {:cont, acc}

          [{^key, segment_id, offset, _size, _timestamp, is_tombstone}] ->
            if is_tombstone do
              {:cont, acc}
            else
              case read_from_segment(segment_id, offset, state) do
                {:ok, value} ->
                  {:cont, [{key, value} | acc]}

                {:error, _reason} ->
                  {:cont, acc}
              end
            end
        end
      end)

    {:reply, {:ok, Enum.reverse(results)}, state}
  end

  @impl true
  def handle_call({:batch_put, kv_pairs}, _from, state) do
    # For now, implement batch put as individual puts for correctness
    # TODO: Optimize this to write as a single batch later
    results =
      Enum.reduce_while(kv_pairs, {state, []}, fn {key, value}, {current_state, acc} ->
        case handle_call({:put, key, value}, self(), current_state) do
          {:reply, {:ok, offset}, new_state} ->
            {:cont, {new_state, [offset | acc]}}

          {:reply, {:error, reason}, _new_state} ->
            {:halt, {:error, reason}}
        end
      end)

    case results do
      {:error, reason} ->
        {:reply, {:error, reason}, state}

      {final_state, offsets} ->
        {:reply, {:ok, List.first(offsets)}, final_state}
    end
  end

  @impl true
  def handle_call(:status, _from, state) do
    status = %{
      data_dir: state.data_dir,
      segment_max_bytes: state.segment_max_bytes,
      sync_on_put: state.sync_on_put,
      active_segment_id: state.active_segment_id,
      active_offset: state.active_offset,
      keydir_size: :ets.info(state.keydir, :size),
      key_set_size: :ets.info(state.key_set, :size)
    }

    {:reply, status, state}
  end

  # Private functions

  defp maybe_rotate_segment(state) do
    case KVStore.Storage.Segment.get_size(state.active_file) do
      {:ok, size} when size >= state.segment_max_bytes ->
        # Close current segment
        KVStore.Storage.Segment.close_segment(
          state.active_file,
          state.active_segment_id,
          state.data_dir
        )

        # Open new segment
        new_segment_id = state.active_segment_id + 1

        case KVStore.Storage.Segment.open_active(state.data_dir, new_segment_id) do
          {:ok, new_file, _path} ->
            Logger.info("Rotated to new segment #{new_segment_id}")
            %{state | active_segment_id: new_segment_id, active_file: new_file, active_offset: 0}

          {:error, reason} ->
            Logger.error("Failed to open new segment: #{inspect(reason)}")
            state
        end

      _ ->
        state
    end
  end

  defp read_from_segment(segment_id, offset, state) do
    if segment_id == state.active_segment_id do
      # Read from active segment by opening a separate read-only file handle
      segment_path = KVStore.Storage.Segment.path(segment_id, state.data_dir)

      case :file.open(segment_path, [:raw, :binary, :read]) do
        {:ok, file} ->
          case KVStore.Storage.Segment.read_record(file, offset) do
            {:ok, record} ->
              :file.close(file)
              {:ok, record.value}

            {:error, :eof} ->
              :file.close(file)
              {:error, :not_found}

            {:error, reason} ->
              :file.close(file)
              {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    else
      # Read from closed segment using file cache
      case KVStore.Storage.FileCache.get_file(segment_id, state.data_dir) do
        {:ok, file} ->
          case KVStore.Storage.Segment.read_record(file, offset) do
            {:ok, record} ->
              {:ok, record.value}

            {:error, reason} ->
              {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp build_batch_data(kv_pairs, state) do
    # For now, we'll write records sequentially
    # In a more optimized version, we could write them as a single batch
    {batch_data, new_state} =
      Enum.reduce(kv_pairs, {[], state}, fn {key, value}, {acc, current_state} ->
        # Check if we need to rotate for each record
        current_state = maybe_rotate_segment(current_state)

        record_data = KVStore.Storage.Record.create(key, value)
        {[record_data | acc], current_state}
      end)

    # Combine all record data
    combined_data = Enum.reverse(batch_data)
    {combined_data, [], new_state}
  end

  defp get_keys_in_range(key_set, start_key, end_key) do
    # Use ETS ordered_set to efficiently get keys in range
    # First check if start_key exists
    case :ets.member(key_set, start_key) do
      true ->
        # start_key exists, start from it
        collect_keys_in_range(key_set, start_key, end_key, [])

      false ->
        # start_key doesn't exist, get next key
        case :ets.next(key_set, start_key) do
          :"$end_of_table" ->
            []

          key when key <= end_key ->
            collect_keys_in_range(key_set, key, end_key, [])

          _ ->
            []
        end
    end
  end

  defp collect_keys_in_range(key_set, current_key, end_key, acc) when current_key <= end_key do
    case :ets.next(key_set, current_key) do
      :"$end_of_table" ->
        Enum.reverse([current_key | acc])

      next_key when next_key <= end_key ->
        collect_keys_in_range(key_set, next_key, end_key, [current_key | acc])

      _ ->
        Enum.reverse([current_key | acc])
    end
  end

  defp open_active_segment(state) do
    segment_path = Path.join(state.data_dir, "#{state.active_segment_id}.data")

    case :file.open(segment_path, [:raw, :binary, :append, :delayed_write]) do
      {:ok, file} ->
        %{state | active_file: file}

      {:error, reason} ->
        Logger.error("Failed to open active segment: #{inspect(reason)}")
        raise "Cannot open active segment"
    end
  end
end
