defmodule KVStore.Storage.Compactor do
  @moduledoc """
  Background compaction process for merging closed segments.

  This process runs in the background to:
  - Merge multiple closed segments into fewer segments
  - Remove tombstones and duplicate keys
  - Create hint files for fast startup
  """

  use GenServer
  require Logger

  # Start merge when 30% of data is stale
  @default_merge_trigger_ratio 0.3
  # Sleep 10ms between batches
  @default_merge_throttle_ms 10

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Client API

  @doc """
  Trigger a manual compaction.
  """
  def compact do
    GenServer.cast(__MODULE__, :compact)
  end

  @doc """
  Get compaction status.
  """
  def status do
    GenServer.call(__MODULE__, :status)
  end

  # Server callbacks

  @impl true
  def init(opts) do
    merge_trigger_ratio = Keyword.get(opts, :merge_trigger_ratio, @default_merge_trigger_ratio)
    merge_throttle_ms = Keyword.get(opts, :merge_throttle_ms, @default_merge_throttle_ms)

    state = %{
      merge_trigger_ratio: merge_trigger_ratio,
      merge_throttle_ms: merge_throttle_ms,
      is_compacting: false,
      last_compaction: nil
    }

    Logger.info("Compactor started with merge_trigger_ratio=#{merge_trigger_ratio}")
    {:ok, state}
  end

  @impl true
  def handle_cast(:compact, state) do
    if state.is_compacting do
      Logger.info("Compaction already in progress, ignoring request")
      {:noreply, state}
    else
      Logger.info("Starting background compaction")
      # TODO: Implement actual compaction logic
      # This will be implemented in later phases
      {:noreply, %{state | is_compacting: true, last_compaction: :os.system_time(:second)}}
    end
  end

  @impl true
  def handle_call(:status, _from, state) do
    status = %{
      is_compacting: state.is_compacting,
      last_compaction: state.last_compaction,
      merge_trigger_ratio: state.merge_trigger_ratio,
      merge_throttle_ms: state.merge_throttle_ms
    }

    {:reply, status, state}
  end

  # Private functions

  defp should_trigger_merge?(_state) do
    # TODO: Implement logic to determine if merge should be triggered
    # This will check the ratio of stale data in closed segments
    false
  end

  defp perform_merge(_state) do
    # TODO: Implement actual merge logic
    # This will:
    # 1. Identify segments to merge
    # 2. Stream keys from oldest to newest
    # 3. Keep only latest non-tombstoned version per key
    # 4. Write to new output segment
    # 5. Create hint file
    # 6. Update ETS entries
    # 7. Delete old segments
    Logger.info("Merge operation not yet implemented")
  end
end
