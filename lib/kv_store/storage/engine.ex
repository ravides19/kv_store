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
    # TODO: Implement put operation
    {:reply, {:ok, :not_implemented_yet}, state}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    # TODO: Implement get operation
    {:reply, {:ok, :not_implemented_yet}, state}
  end

  @impl true
  def handle_call({:delete, key}, _from, state) do
    # TODO: Implement delete operation
    {:reply, {:ok, :not_implemented_yet}, state}
  end

  @impl true
  def handle_call({:range, start_key, end_key}, _from, state) do
    # TODO: Implement range operation
    {:reply, {:ok, :not_implemented_yet}, state}
  end

  @impl true
  def handle_call({:batch_put, kv_pairs}, _from, state) do
    # TODO: Implement batch put operation
    {:reply, {:ok, :not_implemented_yet}, state}
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
