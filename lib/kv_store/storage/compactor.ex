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
  # Minimum segments to trigger compaction
  @min_segments_for_compaction 3

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
      last_compaction: nil,
      compaction_stats: %{
        segments_merged: 0,
        keys_processed: 0,
        tombstones_removed: 0,
        space_saved: 0
      }
    }

    Logger.info("Compactor started with merge_trigger_ratio=#{merge_trigger_ratio}")
    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    status = %{
      is_compacting: state.is_compacting,
      last_compaction: state.last_compaction,
      merge_trigger_ratio: state.merge_trigger_ratio,
      merge_throttle_ms: state.merge_throttle_ms,
      compaction_stats: state.compaction_stats
    }

    {:reply, status, state}
  end

  @impl true
  def handle_cast(:compact, state) do
    if state.is_compacting do
      Logger.info("Compaction already in progress, ignoring request")
      {:noreply, state}
    else
      Logger.info("Starting background compaction")
      # Start compaction in a separate process to avoid blocking
      Task.start(fn -> perform_compaction(state) end)
      {:noreply, %{state | is_compacting: true, last_compaction: :os.system_time(:second)}}
    end
  end

  @impl true
  def handle_cast({:compaction_complete, result}, state) do
    new_stats =
      case result do
        :skipped ->
          state.compaction_stats

        %{segments_merged: merged, tombstones_removed: tombstones} ->
          %{
            state.compaction_stats
            | segments_merged: state.compaction_stats.segments_merged + merged,
              tombstones_removed: state.compaction_stats.tombstones_removed + tombstones
          }
      end

    {:noreply, %{state | is_compacting: false, compaction_stats: new_stats}}
  end

  @impl true
  def handle_cast({:compaction_failed, reason}, state) do
    Logger.error("Compaction failed: #{inspect(reason)}")
    {:noreply, %{state | is_compacting: false}}
  end

  # Private functions

  defp perform_compaction(state) do
    try do
      # Get current engine state
      engine_state = KVStore.Storage.Engine.get_state()

      if should_trigger_merge?(engine_state, state) do
        Logger.info("Merge conditions met, starting compaction")
        result = perform_merge(engine_state, state)

        # Update compactor state
        GenServer.cast(__MODULE__, {:compaction_complete, result})

        Logger.info("Compaction completed: #{inspect(result)}")
      else
        Logger.info("Merge conditions not met, skipping compaction")
        GenServer.cast(__MODULE__, {:compaction_complete, :skipped})
      end
    catch
      _kind, reason ->
        Logger.error("Compaction failed: #{inspect(reason)}")
        GenServer.cast(__MODULE__, {:compaction_failed, reason})
    end
  end

  defp should_trigger_merge?(engine_state, compactor_state) do
    # Get all segments except the active one
    case KVStore.Storage.Segment.list_segments(engine_state.data_dir) do
      {:ok, all_segments} ->
        closed_segments = Enum.filter(all_segments, &(&1 < engine_state.active_segment_id))

        if length(closed_segments) >= @min_segments_for_compaction do
          # Calculate stale data ratio
          stale_ratio = calculate_stale_ratio(closed_segments, engine_state)

          Logger.info(
            "Stale data ratio: #{stale_ratio}, threshold: #{compactor_state.merge_trigger_ratio}"
          )

          stale_ratio >= compactor_state.merge_trigger_ratio
        else
          false
        end

      {:error, _} ->
        false
    end
  end

  defp calculate_stale_ratio(closed_segments, engine_state) do
    # Count total entries and stale entries in closed segments
    {total_entries, stale_entries} =
      Enum.reduce(closed_segments, {0, 0}, fn segment_id, {total, stale} ->
        # Count entries in this segment
        segment_entries = count_segment_entries(segment_id, engine_state)
        # Count stale entries (tombstones or overwritten keys)
        stale_count = count_stale_entries(segment_id, engine_state)
        {total + segment_entries, stale + stale_count}
      end)

    if total_entries > 0 do
      stale_entries / total_entries
    else
      0.0
    end
  end

  defp count_segment_entries(segment_id, engine_state) do
    # Count entries in keydir for this segment
    :ets.match_object(engine_state.keydir, {:_, segment_id, :"$1", :"$2", :"$3", :"$4"})
    |> length()
  end

  defp count_stale_entries(segment_id, engine_state) do
    # Count tombstones and overwritten keys
    entries = :ets.match_object(engine_state.keydir, {:_, segment_id, :"$1", :"$2", :"$3", :"$4"})

    Enum.reduce(entries, 0, fn {key, _seg_id, _offset, _size, _timestamp, is_tombstone}, count ->
      if is_tombstone do
        count + 1
      else
        # Check if this key has a newer version in a later segment
        newer_entries =
          :ets.match_object(engine_state.keydir, {key, :"$1", :"$2", :"$3", :"$4", :"$5"})
          |> Enum.filter(fn {_k, seg_id, _offset, _size, _timestamp, _is_tombstone} ->
            seg_id > segment_id
          end)

        if length(newer_entries) > 0 do
          count + 1
        else
          count
        end
      end
    end)
  end

  defp perform_merge(engine_state, compactor_state) do
    # Get closed segments to merge
    {:ok, all_segments} = KVStore.Storage.Segment.list_segments(engine_state.data_dir)

    closed_segments =
      Enum.filter(all_segments, &(&1 < engine_state.active_segment_id))
      |> Enum.sort()

    Logger.info("Merging #{length(closed_segments)} closed segments: #{inspect(closed_segments)}")

    # Create new output segment
    output_segment_id = find_next_segment_id(engine_state.data_dir)
    output_path = KVStore.Storage.Segment.path(output_segment_id, engine_state.data_dir)

    case :file.open(output_path, [:raw, :binary, :write]) do
      {:ok, output_file} ->
        try do
          result =
            merge_segments_to_file(
              closed_segments,
              output_file,
              output_segment_id,
              engine_state,
              compactor_state
            )

          :file.close(output_file)

          # Create hint file for the new segment
          create_hint_for_merged_segment(output_segment_id, engine_state.data_dir, result.entries)

          # Update ETS entries
          update_ets_entries(closed_segments, output_segment_id, result.entries, engine_state)

          # Delete old segments
          delete_old_segments(closed_segments, engine_state.data_dir)

          result
        catch
          _kind, reason ->
            :file.close(output_file)
            File.rm(output_path)
            raise reason
        end

      {:error, reason} ->
        Logger.error("Failed to create output segment: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp merge_segments_to_file(
         closed_segments,
         output_file,
         output_segment_id,
         engine_state,
         _compactor_state
       ) do
    # Collect all entries from closed segments, keeping only the latest version of each key
    entries_map = collect_latest_entries(closed_segments, engine_state)

    # Write entries to output file in sorted order
    {entries_written, total_size, tombstones_removed} =
      Enum.reduce(entries_map, {[], 0, 0}, fn {key, entry}, {entries, size, tombstones} ->
        case entry do
          {_key, _seg_id, _offset, _size, _timestamp, true} ->
            # Tombstone entry
            {entries, size, tombstones + 1}

          {_key, _seg_id, _offset, _size, timestamp, false, value} ->
            # Regular entry with value
            record_data = KVStore.Storage.Record.create(key, value)

            case :file.write(output_file, record_data) do
              :ok ->
                record_size = IO.iodata_length(record_data)
                new_entry = {key, output_segment_id, size, record_size, timestamp, false}
                {[new_entry | entries], size + record_size, tombstones}

              {:error, reason} ->
                Logger.error("Failed to write record for key #{inspect(key)}: #{inspect(reason)}")
                {entries, size, tombstones}
            end
        end
      end)

    %{
      entries: Enum.reverse(entries_written),
      total_size: total_size,
      tombstones_removed: tombstones_removed,
      segments_merged: length(closed_segments)
    }
  end

  defp collect_latest_entries(closed_segments, engine_state) do
    # Get all entries from closed segments
    all_entries =
      Enum.flat_map(closed_segments, fn segment_id ->
        :ets.match_object(engine_state.keydir, {:_, segment_id, :"$1", :"$2", :"$3", :"$4"})
        |> Enum.map(fn {key, seg_id, offset, size, timestamp, is_tombstone} ->
          {key, {key, seg_id, offset, size, timestamp, is_tombstone}}
        end)
      end)

    # Group by key and keep only the latest version
    entries_map =
      Enum.reduce(all_entries, %{}, fn {key, entry}, acc ->
        case Map.get(acc, key) do
          nil ->
            Map.put(acc, key, entry)

          existing_entry ->
            # Keep the one with the latest timestamp
            {_k, _seg_id, _offset, _size, existing_timestamp, _is_tombstone} = existing_entry
            {_k, _seg_id, _offset, _size, new_timestamp, _is_tombstone} = entry

            if new_timestamp > existing_timestamp do
              Map.put(acc, key, entry)
            else
              acc
            end
        end
      end)

    # Read actual values for non-tombstone entries
    Enum.reduce(entries_map, %{}, fn {key, entry}, acc ->
      {_k, seg_id, offset, size, timestamp, is_tombstone} = entry

      if is_tombstone do
        Map.put(acc, key, entry)
      else
        # Read the actual value from the segment
        case read_value_from_segment(seg_id, offset, engine_state) do
          {:ok, value} ->
            Map.put(acc, key, {key, seg_id, offset, size, timestamp, is_tombstone, value})

          {:error, _reason} ->
            # Skip entries that can't be read
            acc
        end
      end
    end)
  end

  defp read_value_from_segment(segment_id, offset, engine_state) do
    # Use the file cache to get the file handle
    case KVStore.Storage.FileCache.get_file(segment_id, engine_state.data_dir) do
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

  defp create_hint_for_merged_segment(segment_id, data_dir, entries) do
    # Convert entries to hint format
    hint_entries =
      Enum.map(entries, fn {key, seg_id, offset, size, timestamp, is_tombstone} ->
        {key, seg_id, offset, size, timestamp, is_tombstone}
      end)

    case KVStore.Storage.Hint.create_hint(segment_id, data_dir, hint_entries) do
      {:ok, hint_path} ->
        Logger.info("Created hint file for merged segment #{segment_id}: #{hint_path}")
        :ok

      {:error, reason} ->
        Logger.error(
          "Failed to create hint file for merged segment #{segment_id}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp update_ets_entries(old_segments, _new_segment_id, new_entries, engine_state) do
    # Remove old entries
    Enum.each(old_segments, fn segment_id ->
      :ets.match_delete(engine_state.keydir, {:_, segment_id, :"$1", :"$2", :"$3", :"$4"})
    end)

    # Add new entries
    Enum.each(new_entries, fn {key, seg_id, offset, size, timestamp, is_tombstone} ->
      :ets.insert(engine_state.keydir, {key, seg_id, offset, size, timestamp, is_tombstone})

      # Update key_set (only for non-tombstone entries)
      if not is_tombstone do
        :ets.insert(engine_state.key_set, {key, true})
      end
    end)

    Logger.info(
      "Updated ETS entries: removed #{length(old_segments)} segments, added #{length(new_entries)} entries"
    )
  end

  defp delete_old_segments(closed_segments, data_dir) do
    Enum.each(closed_segments, fn segment_id ->
      # Delete segment file
      KVStore.Storage.Segment.delete(segment_id, data_dir)

      # Delete hint file
      KVStore.Storage.Hint.delete_hint(segment_id, data_dir)
    end)

    Logger.info("Deleted #{length(closed_segments)} old segments and their hint files")
  end

  defp find_next_segment_id(data_dir) do
    case KVStore.Storage.Segment.list_segments(data_dir) do
      {:ok, segments} ->
        if segments == [] do
          1
        else
          Enum.max(segments) + 1
        end

      {:error, _reason} ->
        1
    end
  end
end
