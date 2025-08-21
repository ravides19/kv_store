defmodule KVStore.Storage.Recovery do
  @moduledoc """
  Crash recovery module for the KVStore system.

  This module handles:
  - Detecting and repairing corrupted segments
  - Rebuilding indexes from hint files and data files
  - Validating data integrity after crashes
  - Truncating partially written records
  - Fast startup recovery
  """

  require Logger

  @doc """
  Perform crash recovery for the given data directory.
  Returns {:ok, recovery_info} or {:error, reason}.
  """
  def recover(data_dir) do
    Logger.info("Starting crash recovery for directory: #{data_dir}")

    start_time = :os.system_time(:millisecond)

    try do
      # Step 1: Scan and validate all segments
      {:ok, segments} = scan_segments(data_dir)

      # Step 2: Validate and repair segments if needed
      {:ok, validated_segments} = validate_segments(segments, data_dir)

      # Step 3: Load data from hint files and validated segments
      {:ok, recovery_data} = load_recovery_data(validated_segments, data_dir)

      # Step 4: Build recovery statistics
      end_time = :os.system_time(:millisecond)
      recovery_time = end_time - start_time

      recovery_info = %{
        segments_found: length(segments),
        segments_valid: length(validated_segments),
        segments_repaired: length(segments) - length(validated_segments),
        entries_recovered: recovery_data.entries_count,
        tombstones_found: recovery_data.tombstones_count,
        recovery_time_ms: recovery_time,
        keydir_entries: recovery_data.keydir_entries,
        key_set_entries: recovery_data.key_set_entries
      }

      Logger.info("Crash recovery completed successfully in #{recovery_time}ms")
      Logger.info("Recovery summary: #{inspect(recovery_info)}")

      {:ok, recovery_info, recovery_data}
    catch
      kind, reason ->
        Logger.error("Crash recovery failed: #{inspect(reason)}")
        {:error, {kind, reason}}
    end
  end

  @doc """
  Scan the data directory for segment files.
  """
  def scan_segments(data_dir) do
    case KVStore.Storage.Segment.list_segments(data_dir) do
      {:ok, segment_ids} ->
        segments =
          Enum.map(segment_ids, fn segment_id ->
            path = KVStore.Storage.Segment.path(segment_id, data_dir)

            %{
              id: segment_id,
              path: path,
              exists: File.exists?(path)
            }
          end)

        existing_segments = Enum.filter(segments, & &1.exists)
        Logger.info("Found #{length(existing_segments)} segment files")

        {:ok, existing_segments}

      {:error, reason} ->
        Logger.error("Failed to scan segments: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Validate segment files and repair if necessary.
  """
  def validate_segments(segments, data_dir) do
    Logger.info("Validating #{length(segments)} segments")

    validated_segments =
      Enum.reduce(segments, [], fn segment, acc ->
        case validate_segment(segment, data_dir) do
          {:ok, validated_segment} ->
            [validated_segment | acc]

          {:error, reason} ->
            Logger.warning("Segment #{segment.id} failed validation: #{inspect(reason)}")
            acc
        end
      end)

    {:ok, Enum.reverse(validated_segments)}
  end

  @doc """
  Validate a single segment file.
  """
  def validate_segment(segment, data_dir) do
    Logger.debug("Validating segment #{segment.id}")

    case :file.open(segment.path, [:raw, :binary, :read]) do
      {:ok, file} ->
        try do
          case scan_segment_records(file, segment.id, data_dir) do
            {:ok, validation_result} ->
              :file.close(file)

              validated_segment = Map.merge(segment, validation_result)

              Logger.debug(
                "Segment #{segment.id} validation complete: #{validation_result.records_count} records"
              )

              {:ok, validated_segment}

            {:error, reason} ->
              :file.close(file)
              {:error, reason}
          end
        catch
          kind, error ->
            :file.close(file)
            {:error, {kind, error}}
        end

      {:error, reason} ->
        Logger.error("Failed to open segment #{segment.id}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Scan all records in a segment and validate them.
  """
  def scan_segment_records(file, segment_id, data_dir) do
    {:ok, file_size} = :file.position(file, :eof)
    :file.position(file, :bof)

    scan_result =
      scan_records_from_offset(file, 0, file_size, %{
        records_count: 0,
        valid_records: [],
        last_valid_offset: 0,
        corruption_detected: false,
        corruption_offset: nil
      })

    case scan_result do
      %{corruption_detected: true, corruption_offset: corruption_offset} ->
        Logger.warning(
          "Corruption detected in segment #{segment_id} at offset #{corruption_offset}"
        )

        # Truncate the file at the last valid offset
        case truncate_segment_at_offset(data_dir, segment_id, scan_result.last_valid_offset) do
          :ok ->
            Logger.info(
              "Truncated segment #{segment_id} at offset #{scan_result.last_valid_offset}"
            )

            {:ok, Map.put(scan_result, :truncated, true)}

          {:error, reason} ->
            {:error, reason}
        end

      %{corruption_detected: false} ->
        {:ok, Map.put(scan_result, :truncated, false)}
    end
  end

  defp scan_records_from_offset(_file, offset, file_size, acc) when offset >= file_size do
    acc
  end

  defp scan_records_from_offset(file, offset, file_size, acc) do
    case KVStore.Storage.Record.read(file, offset) do
      {:ok, record} ->
        # Validate record integrity
        case validate_record_integrity(record) do
          :ok ->
            new_acc = %{
              acc
              | records_count: acc.records_count + 1,
                valid_records: [record | acc.valid_records],
                last_valid_offset: offset + record.total_size
            }

            scan_records_from_offset(file, offset + record.total_size, file_size, new_acc)

          {:error, reason} ->
            Logger.debug("Invalid record at offset #{offset}: #{inspect(reason)}")
            %{acc | corruption_detected: true, corruption_offset: offset}
        end

      {:error, :eof} ->
        # Reached end of file
        acc

      {:error, reason} ->
        Logger.debug("Failed to read record at offset #{offset}: #{inspect(reason)}")
        %{acc | corruption_detected: true, corruption_offset: offset}
    end
  end

  defp validate_record_integrity(record) do
    cond do
      # Validate magic number
      record.magic != 0xCAFEBABE ->
        {:error, :invalid_magic}

      # Validate version
      record.version != 0x01 ->
        {:error, :invalid_version}

      # Validate timestamp (should be reasonable)
      record.timestamp < 0 or record.timestamp > :os.system_time(:millisecond) + 86_400_000 ->
        {:error, :invalid_timestamp}

      # Validate key and value are present
      record.key == nil or (not record.is_tombstone and record.value == nil) ->
        {:error, :missing_data}

      true ->
        :ok
    end
  end

  defp truncate_segment_at_offset(data_dir, segment_id, offset) do
    segment_path = KVStore.Storage.Segment.path(segment_id, data_dir)

    # Create backup before truncation
    backup_path = "#{segment_path}.backup.#{:os.system_time(:second)}"

    case File.cp(segment_path, backup_path) do
      :ok ->
        Logger.info("Created backup: #{backup_path}")

        # Read valid data
        case File.read(segment_path) do
          {:ok, data} ->
            valid_data = binary_part(data, 0, offset)

            # Write truncated data
            case File.write(segment_path, valid_data) do
              :ok ->
                Logger.info("Successfully truncated segment #{segment_id} to #{offset} bytes")
                :ok

              {:error, reason} ->
                Logger.error("Failed to write truncated segment: #{inspect(reason)}")
                # Restore from backup
                File.cp(backup_path, segment_path)
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        Logger.error("Failed to create backup: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Load recovery data from validated segments.
  """
  def load_recovery_data(validated_segments, data_dir) do
    Logger.info("Loading recovery data from #{length(validated_segments)} validated segments")

    # Create ETS tables for recovery
    keydir = :ets.new(:recovery_keydir, [:set, :public])
    key_set = :ets.new(:recovery_key_set, [:ordered_set, :public])

    # Load data in segment order (oldest first)
    sorted_segments = Enum.sort_by(validated_segments, & &1.id)

    {entries_count, tombstones_count} =
      Enum.reduce(sorted_segments, {0, 0}, fn segment, {entries_acc, tombstones_acc} ->
        case load_segment_data(segment, keydir, key_set, data_dir) do
          {:ok, segment_stats} ->
            {entries_acc + segment_stats.entries, tombstones_acc + segment_stats.tombstones}

          {:error, reason} ->
            Logger.warning("Failed to load segment #{segment.id}: #{inspect(reason)}")
            {entries_acc, tombstones_acc}
        end
      end)

    recovery_data = %{
      keydir: keydir,
      key_set: key_set,
      keydir_entries: :ets.info(keydir, :size),
      key_set_entries: :ets.info(key_set, :size),
      entries_count: entries_count,
      tombstones_count: tombstones_count
    }

    {:ok, recovery_data}
  end

  defp load_segment_data(segment, keydir, key_set, data_dir) do
    # Try to load from hint file first (faster)
    case load_from_hint_file(segment, keydir, key_set, data_dir) do
      {:ok, stats} ->
        Logger.debug("Loaded segment #{segment.id} from hint file")
        {:ok, stats}

      {:error, _reason} ->
        # Fall back to scanning the data file
        Logger.debug("Hint file not available for segment #{segment.id}, scanning data file")
        load_from_data_file(segment, keydir, key_set)
    end
  end

  defp load_from_hint_file(segment, keydir, key_set, data_dir) do
    case KVStore.Storage.Hint.read_hint(segment.id, data_dir) do
      {:ok, hint_entries} ->
        entries_count = length(hint_entries)

        tombstones_count =
          Enum.count(hint_entries, fn {_key, _seg_id, _offset, _size, _timestamp, is_tombstone} ->
            is_tombstone
          end)

        # Load entries into ETS tables
        Enum.each(hint_entries, fn {key, seg_id, offset, size, timestamp, is_tombstone} ->
          :ets.insert(keydir, {key, seg_id, offset, size, timestamp, is_tombstone})

          # Only add non-tombstone keys to key_set
          if not is_tombstone do
            :ets.insert(key_set, {key, true})
          end
        end)

        {:ok, %{entries: entries_count, tombstones: tombstones_count}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp load_from_data_file(segment, keydir, key_set) do
    case :file.open(segment.path, [:raw, :binary, :read]) do
      {:ok, file} ->
        try do
          result =
            scan_data_file_for_recovery(file, segment.id, keydir, key_set, 0, %{
              entries: 0,
              tombstones: 0
            })

          :file.close(file)
          {:ok, result}
        catch
          kind, error ->
            :file.close(file)
            {:error, {kind, error}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp scan_data_file_for_recovery(file, segment_id, keydir, key_set, offset, stats) do
    case KVStore.Storage.Record.read(file, offset) do
      {:ok, record} ->
        # Insert into keydir
        :ets.insert(keydir, {
          record.key,
          segment_id,
          offset,
          # Subtract header size to get payload size
          record.total_size - 30,
          record.timestamp,
          record.is_tombstone
        })

        # Insert into key_set if not tombstone
        if not record.is_tombstone do
          :ets.insert(key_set, {record.key, true})
        end

        new_stats =
          if record.is_tombstone do
            %{stats | tombstones: stats.tombstones + 1}
          else
            %{stats | entries: stats.entries + 1}
          end

        scan_data_file_for_recovery(
          file,
          segment_id,
          keydir,
          key_set,
          offset + record.total_size,
          new_stats
        )

      {:error, :eof} ->
        stats

      {:error, _reason} ->
        # Stop at first error (already validated, so this shouldn't happen)
        stats
    end
  end

  @doc """
  Verify the integrity of the entire data directory.
  """
  def verify_integrity(data_dir) do
    Logger.info("Starting integrity verification for #{data_dir}")

    case scan_segments(data_dir) do
      {:ok, segments} ->
        verification_results =
          Enum.map(segments, fn segment ->
            case verify_segment_integrity(segment) do
              {:ok, result} -> {segment.id, :ok, result}
              {:error, reason} -> {segment.id, :error, reason}
            end
          end)

        failed_segments =
          Enum.filter(verification_results, fn {_id, status, _result} -> status == :error end)

        if length(failed_segments) == 0 do
          Logger.info("Integrity verification passed for all segments")
          {:ok, :all_valid}
        else
          Logger.warning("Integrity verification failed for #{length(failed_segments)} segments")
          {:error, {:integrity_check_failed, failed_segments}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp verify_segment_integrity(segment) do
    case :file.open(segment.path, [:raw, :binary, :read]) do
      {:ok, file} ->
        try do
          result = verify_all_records(file, 0, %{valid: 0, invalid: 0})
          :file.close(file)
          {:ok, result}
        catch
          kind, error ->
            :file.close(file)
            {:error, {kind, error}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp verify_all_records(file, offset, stats) do
    case KVStore.Storage.Record.read(file, offset) do
      {:ok, record} ->
        case validate_record_integrity(record) do
          :ok ->
            new_stats = %{stats | valid: stats.valid + 1}
            verify_all_records(file, offset + record.total_size, new_stats)

          {:error, _reason} ->
            new_stats = %{stats | invalid: stats.invalid + 1}
            verify_all_records(file, offset + record.total_size, new_stats)
        end

      {:error, :eof} ->
        stats

      {:error, _reason} ->
        %{stats | invalid: stats.invalid + 1}
    end
  end
end
