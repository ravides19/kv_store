defmodule KVStore.Storage.FileCache do
  @moduledoc """
  LRU cache for managing open file handles to closed segments.

  This cache helps avoid repeatedly opening and closing files
  when reading from closed segments during compaction or recovery.
  """

  use GenServer
  require Logger

  @default_max_files 10

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Client API

  @doc """
  Get a file handle for a segment. Opens the file if not cached.
  """
  def get_file(segment_id, data_dir) do
    GenServer.call(__MODULE__, {:get_file, segment_id, data_dir})
  end

  @doc """
  Close a specific file handle.
  """
  def close_file(segment_id) do
    GenServer.call(__MODULE__, {:close_file, segment_id})
  end

  # Server callbacks

  @impl true
  def init(opts) do
    max_files = Keyword.get(opts, :max_files, @default_max_files)

    state = %{
      max_files: max_files,
      # segment_id => {file_handle, last_used}
      files: %{},
      # list of segment_ids in LRU order
      lru: []
    }

    Logger.info("File cache started with max_files=#{max_files}")
    {:ok, state}
  end

  @impl true
  def handle_call({:get_file, segment_id, data_dir}, _from, state) do
    case Map.get(state.files, segment_id) do
      nil ->
        # File not in cache, open it
        case open_segment_file(segment_id, data_dir) do
          {:ok, file} ->
            state = add_file_to_cache(state, segment_id, file)
            {:reply, {:ok, file}, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      {file, _last_used} ->
        # File in cache, update LRU
        state = update_lru(state, segment_id)
        {:reply, {:ok, file}, state}
    end
  end

  @impl true
  def handle_call({:close_file, segment_id}, _from, state) do
    case Map.get(state.files, segment_id) do
      nil ->
        {:reply, :ok, state}

      {file, _last_used} ->
        :file.close(file)
        state = remove_file_from_cache(state, segment_id)
        {:reply, :ok, state}
    end
  end

  # Private functions

  defp open_segment_file(segment_id, data_dir) do
    file_path = Path.join(data_dir, "#{segment_id}.data")
    :file.open(file_path, [:raw, :binary, :read])
  end

  defp add_file_to_cache(state, segment_id, file) do
    # If cache is full, evict least recently used
    state =
      if map_size(state.files) >= state.max_files do
        evict_lru(state)
      else
        state
      end

    # Add new file to cache
    files = Map.put(state.files, segment_id, {file, :os.system_time(:millisecond)})
    lru = [segment_id | state.lru]

    %{state | files: files, lru: lru}
  end

  defp update_lru(state, segment_id) do
    lru = [segment_id | Enum.reject(state.lru, &(&1 == segment_id))]

    files =
      Map.update!(state.files, segment_id, fn {file, _} ->
        {file, :os.system_time(:millisecond)}
      end)

    %{state | files: files, lru: lru}
  end

  defp remove_file_from_cache(state, segment_id) do
    files = Map.delete(state.files, segment_id)
    lru = Enum.reject(state.lru, &(&1 == segment_id))

    %{state | files: files, lru: lru}
  end

  defp evict_lru(state) do
    case state.lru do
      [] ->
        state

      [segment_id | rest_lru] ->
        case Map.get(state.files, segment_id) do
          {file, _} ->
            :file.close(file)
            files = Map.delete(state.files, segment_id)
            %{state | files: files, lru: rest_lru}

          nil ->
            %{state | lru: rest_lru}
        end
    end
  end
end
