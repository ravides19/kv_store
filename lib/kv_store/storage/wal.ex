defmodule KVStore.Storage.WAL do
  @moduledoc """
  Write-Ahead Log (WAL) for ensuring durability and atomicity.

  The WAL ensures that all operations are logged before being applied,
  providing crash recovery and atomic operation guarantees.
  """

  require Logger

  # WAL record types
  @wal_put 0x01
  @wal_delete 0x02
  @wal_batch_start 0x03
  @wal_batch_end 0x04
  @wal_checkpoint 0x05

  # WAL file magic number and version
  @wal_magic 0xDEADBEEF
  @wal_version 0x01
  # magic(4) + version(1) + type(1) + timestamp(8) + size(4) + checksum(4)
  @wal_header_size 22

  defstruct [:file, :path, :offset, :sync_policy]

  @doc """
  Open or create a WAL file.
  """
  def open(data_dir, sync_policy \\ :sync_on_write) do
    wal_path = Path.join(data_dir, "wal.log")

    case :file.open(wal_path, [:raw, :binary, :append, :read]) do
      {:ok, file} ->
        # Get current file size
        {:ok, offset} = :file.position(file, :eof)

        wal = %__MODULE__{
          file: file,
          path: wal_path,
          offset: offset,
          sync_policy: sync_policy
        }

        Logger.info("Opened WAL file: #{wal_path} at offset #{offset}")
        {:ok, wal}

      {:error, reason} ->
        Logger.error("Failed to open WAL file: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Close the WAL file.
  """
  def close(%__MODULE__{file: file} = wal) do
    # Ensure all data is flushed to disk before closing
    case :file.sync(file) do
      :ok ->
        case :file.close(file) do
          :ok ->
            Logger.info("Closed WAL file: #{wal.path}")
            :ok

          {:error, reason} ->
            Logger.error("Failed to close WAL file: #{inspect(reason)}")
            {:error, reason}
        end

      {:error, reason} ->
        Logger.error("Failed to sync WAL file: #{inspect(reason)}")
        # Still try to close the file
        :file.close(file)
        {:error, reason}
    end
  end

  @doc """
  Write a PUT operation to the WAL.
  """
  def write_put(wal, key, value, transaction_id \\ nil) do
    entry_data = %{
      key: key,
      value: value,
      transaction_id: transaction_id
    }

    write_entry(wal, @wal_put, entry_data)
  end

  @doc """
  Write a DELETE operation to the WAL.
  """
  def write_delete(wal, key, transaction_id \\ nil) do
    entry_data = %{
      key: key,
      transaction_id: transaction_id
    }

    write_entry(wal, @wal_delete, entry_data)
  end

  @doc """
  Write a batch start marker to the WAL.
  """
  def write_batch_start(wal, transaction_id) do
    entry_data = %{
      transaction_id: transaction_id,
      # Will be updated on batch_end
      batch_size: 0
    }

    write_entry(wal, @wal_batch_start, entry_data)
  end

  @doc """
  Write a batch end marker to the WAL.
  """
  def write_batch_end(wal, transaction_id, batch_size) do
    entry_data = %{
      transaction_id: transaction_id,
      batch_size: batch_size
    }

    write_entry(wal, @wal_batch_end, entry_data)
  end

  @doc """
  Write a checkpoint marker to the WAL.
  """
  def write_checkpoint(wal, checkpoint_data) do
    write_entry(wal, @wal_checkpoint, checkpoint_data)
  end

  defp write_entry(%__MODULE__{} = wal, entry_type, entry_data) do
    timestamp = :os.system_time(:millisecond)

    # Serialize the entry data
    serialized_data = :erlang.term_to_binary(entry_data)
    data_size = byte_size(serialized_data)

    # Build the header
    header = build_wal_header(entry_type, timestamp, data_size, serialized_data)

    # Write header and data
    wal_record = [header, serialized_data]

    case :file.write(wal.file, wal_record) do
      :ok ->
        new_offset = wal.offset + @wal_header_size + data_size
        new_wal = %{wal | offset: new_offset}

        # Sync if required
        case wal.sync_policy do
          :sync_on_write ->
            case :file.sync(wal.file) do
              :ok -> {:ok, new_wal}
              {:error, reason} -> {:error, reason}
            end

          :no_sync ->
            {:ok, new_wal}

          {:sync_every_n, n} when rem(new_offset, n) == 0 ->
            case :file.sync(wal.file) do
              :ok -> {:ok, new_wal}
              {:error, reason} -> {:error, reason}
            end

          _ ->
            {:ok, new_wal}
        end

      {:error, reason} ->
        Logger.error("Failed to write WAL entry: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp build_wal_header(entry_type, timestamp, data_size, data) do
    checksum = :erlang.crc32(data)

    <<
      @wal_magic::32,
      @wal_version::8,
      entry_type::8,
      timestamp::64,
      data_size::32,
      checksum::32
    >>
  end

  @doc """
  Read and replay WAL entries for recovery.
  """
  def replay(wal_path, callback_fn) do
    Logger.info("Replaying WAL: #{wal_path}")

    case File.exists?(wal_path) do
      false ->
        Logger.info("No WAL file found, starting fresh")
        {:ok, :no_wal}

      true ->
        case :file.open(wal_path, [:raw, :binary, :read]) do
          {:ok, file} ->
            try do
              result = replay_entries(file, 0, callback_fn, %{entries: 0, errors: 0})
              :file.close(file)

              Logger.info(
                "WAL replay completed: #{result.entries} entries, #{result.errors} errors"
              )

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
  end

  defp replay_entries(file, offset, callback_fn, stats) do
    case read_wal_entry(file, offset) do
      {:ok, entry, next_offset} ->
        # Call the callback function with the entry
        case callback_fn.(entry) do
          :ok ->
            new_stats = %{stats | entries: stats.entries + 1}
            replay_entries(file, next_offset, callback_fn, new_stats)

          {:error, reason} ->
            Logger.warning(
              "WAL replay callback failed for entry at offset #{offset}: #{inspect(reason)}"
            )

            new_stats = %{stats | errors: stats.errors + 1}
            replay_entries(file, next_offset, callback_fn, new_stats)
        end

      {:error, :eof} ->
        stats

      {:error, reason} ->
        Logger.warning("Failed to read WAL entry at offset #{offset}: #{inspect(reason)}")
        %{stats | errors: stats.errors + 1}
    end
  end

  defp read_wal_entry(file, offset) do
    # Read the header
    case :file.pread(file, offset, @wal_header_size) do
      {:ok, header_data} ->
        case parse_wal_header(header_data) do
          {:ok, header} ->
            # Read the data
            case :file.pread(file, offset + @wal_header_size, header.data_size) do
              {:ok, entry_data} ->
                # Verify checksum
                if :erlang.crc32(entry_data) == header.checksum do
                  try do
                    parsed_data = :erlang.binary_to_term(entry_data)

                    entry = %{
                      type: header.entry_type,
                      timestamp: header.timestamp,
                      data: parsed_data,
                      offset: offset
                    }

                    next_offset = offset + @wal_header_size + header.data_size
                    {:ok, entry, next_offset}
                  catch
                    _kind, _error ->
                      {:error, :invalid_data}
                  end
                else
                  {:error, :checksum_mismatch}
                end

              :eof ->
                {:error, :eof}

              {:error, :eof} ->
                {:error, :eof}

              {:error, reason} ->
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end

      :eof ->
        {:error, :eof}

      {:error, :eof} ->
        {:error, :eof}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_wal_header(<<
         @wal_magic::32,
         @wal_version::8,
         entry_type::8,
         timestamp::64,
         data_size::32,
         checksum::32
       >>) do
    {:ok,
     %{
       magic: @wal_magic,
       version: @wal_version,
       entry_type: entry_type,
       timestamp: timestamp,
       data_size: data_size,
       checksum: checksum
     }}
  end

  defp parse_wal_header(_invalid_header) do
    {:error, :invalid_header}
  end

  @doc """
  Truncate the WAL file (usually after successful checkpoint).
  """
  def truncate(wal_path) do
    case File.write(wal_path, "") do
      :ok ->
        Logger.info("Truncated WAL file: #{wal_path}")
        :ok

      {:error, reason} ->
        Logger.error("Failed to truncate WAL file: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Get WAL entry types for pattern matching.
  """
  def entry_types do
    %{
      put: @wal_put,
      delete: @wal_delete,
      batch_start: @wal_batch_start,
      batch_end: @wal_batch_end,
      checkpoint: @wal_checkpoint
    }
  end
end
