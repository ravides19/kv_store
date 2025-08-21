defmodule KVStore.Storage.Segment do
  @moduledoc """
  Segment management for the storage engine.

  Handles:
  - Active segment file management
  - Segment rotation when size threshold is reached
  - Basic read/write operations on segments
  """

  require Logger

  @doc """
  Open or create a new active segment.
  """
  def open_active(data_dir, segment_id) do
    segment_path = Path.join(data_dir, "#{segment_id}.data")

    case :file.open(segment_path, [:raw, :binary, :append, :delayed_write]) do
      {:ok, file} ->
        Logger.info("Opened active segment #{segment_id} at #{segment_path}")
        {:ok, file, segment_path}

      {:error, reason} ->
        Logger.error("Failed to open active segment #{segment_id}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Close a segment and mark it as immutable.
  """
  def close_segment(file, segment_id, _data_dir) do
    case :file.close(file) do
      :ok ->
        Logger.info("Closed segment #{segment_id}")
        {:ok, segment_id}

      {:error, reason} ->
        Logger.error("Failed to close segment #{segment_id}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Write a record to the active segment.
  Returns the offset where the record was written.
  """
  def write_record(file, record_data, current_offset) do
    case :file.write(file, record_data) do
      :ok ->
        record_size = IO.iodata_length(record_data)
        new_offset = current_offset + record_size
        {:ok, current_offset, new_offset}

      {:error, reason} ->
        Logger.error("Failed to write record: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Read a record from a segment at the given offset.
  """
  def read_record(file, offset) do
    KVStore.Storage.Record.read(file, offset)
  end

  @doc """
  Get the current size of a segment file.
  """
  def get_size(file) do
    case :file.position(file, :eof) do
      {:ok, size} ->
        # Reset position to end for appending
        :file.position(file, {:end, 0})
        {:ok, size}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Sync the segment file to disk.
  """
  def sync(file) do
    case :file.sync(file) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("Failed to sync segment: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Check if a segment file exists.
  """
  def exists?(segment_id, data_dir) do
    segment_path = Path.join(data_dir, "#{segment_id}.data")
    File.exists?(segment_path)
  end

  @doc """
  Get the path to a segment file.
  """
  def path(segment_id, data_dir) do
    Path.join(data_dir, "#{segment_id}.data")
  end

  @doc """
  List all segment files in the data directory.
  """
  def list_segments(data_dir) do
    case File.ls(data_dir) do
      {:ok, files} ->
        segments =
          files
          |> Enum.filter(&String.ends_with?(&1, ".data"))
          |> Enum.map(fn file ->
            case Integer.parse(String.trim(file, ".data")) do
              {id, ""} -> id
              _ -> nil
            end
          end)
          |> Enum.filter(&(&1 != nil))
          |> Enum.sort()

        {:ok, segments}

      {:error, reason} ->
        Logger.error("Failed to list segments: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Delete a segment file.
  """
  def delete(segment_id, data_dir) do
    segment_path = path(segment_id, data_dir)

    case File.rm(segment_path) do
      :ok ->
        Logger.info("Deleted segment #{segment_id}")
        :ok

      {:error, reason} ->
        Logger.error("Failed to delete segment #{segment_id}: #{inspect(reason)}")
        {:error, reason}
    end
  end
end
