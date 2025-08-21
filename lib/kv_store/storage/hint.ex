defmodule KVStore.Storage.Hint do
  @moduledoc """
  Hint file management for fast startup recovery.

  Hint files contain a subset of the keydir information (key, segment_id, offset, size)
  but exclude the actual values, allowing for fast index reconstruction without
  scanning all data files.
  """

  require Logger
  import Bitwise

  @hint_magic_number 0xCAFEBABE
  @hint_version 0x01
  @hint_header_size 13

  @doc """
  Create a hint file for a segment.
  Returns {:ok, hint_path} or {:error, reason}.
  """
  def create_hint(segment_id, data_dir, keydir_entries) do
    hint_path = hint_path(segment_id, data_dir)

    case :file.open(hint_path, [:raw, :binary, :write]) do
      {:ok, file} ->
        try do
          # Write hint file header
          header = build_hint_header(length(keydir_entries))
          :file.write(file, header)

          # Write hint entries
          Enum.each(keydir_entries, fn {key, segment_id, offset, size, timestamp, is_tombstone} ->
            entry = build_hint_entry(key, segment_id, offset, size, timestamp, is_tombstone)
            :file.write(file, entry)
          end)

          :file.close(file)
          Logger.info("Created hint file for segment #{segment_id}: #{hint_path}")
          {:ok, hint_path}
        catch
          _kind, reason ->
            :file.close(file)

            Logger.error(
              "Failed to create hint file for segment #{segment_id}: #{inspect(reason)}"
            )

            {:error, reason}
        end

      {:error, reason} ->
        Logger.error("Failed to open hint file for writing: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Read hint file and return keydir entries.
  Returns {:ok, entries} or {:error, reason}.
  """
  def read_hint(segment_id, data_dir) do
    hint_path = hint_path(segment_id, data_dir)

    case :file.open(hint_path, [:raw, :binary, :read]) do
      {:ok, file} ->
        try do
          case read_hint_header(file) do
            {:ok, num_entries} ->
              case read_hint_entries(file, num_entries, []) do
                {:ok, entries} ->
                  :file.close(file)
                  {:ok, entries}

                {:error, reason} ->
                  :file.close(file)
                  {:error, reason}
              end

            {:error, reason} ->
              :file.close(file)
              {:error, reason}
          end
        catch
          _kind, reason ->
            :file.close(file)
            Logger.error("Failed to read hint file #{hint_path}: #{inspect(reason)}")
            {:error, reason}
        end

      {:error, :enoent} ->
        # Hint file doesn't exist, which is normal for segments without hints
        {:ok, []}

      {:error, reason} ->
        Logger.error("Failed to open hint file for reading: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  List all hint files in the data directory.
  Returns {:ok, segment_ids} or {:error, reason}.
  """
  def list_hints(data_dir) do
    case File.ls(data_dir) do
      {:ok, files} ->
        hints =
          files
          |> Enum.filter(&String.ends_with?(&1, ".hint"))
          |> Enum.map(fn file ->
            case Integer.parse(String.trim(file, ".hint")) do
              {id, ""} -> id
              _ -> nil
            end
          end)
          |> Enum.filter(&(&1 != nil))
          |> Enum.sort()

        {:ok, hints}

      {:error, reason} ->
        Logger.error("Failed to list hint files: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Delete a hint file.
  Returns :ok or {:error, reason}.
  """
  def delete_hint(segment_id, data_dir) do
    hint_path = hint_path(segment_id, data_dir)

    case File.rm(hint_path) do
      :ok ->
        Logger.info("Deleted hint file: #{hint_path}")
        :ok

      {:error, :enoent} ->
        # File doesn't exist, which is fine
        :ok

      {:error, reason} ->
        Logger.error("Failed to delete hint file #{hint_path}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc """
  Check if a hint file exists.
  """
  def hint_exists?(segment_id, data_dir) do
    hint_path = hint_path(segment_id, data_dir)
    File.exists?(hint_path)
  end

  @doc """
  Get the path to a hint file.
  """
  def hint_path(segment_id, data_dir) do
    Path.join(data_dir, "#{segment_id}.hint")
  end

  # Private functions

  defp build_hint_header(num_entries) do
    <<
      @hint_magic_number::32-big,
      @hint_version::8,
      num_entries::32-big,
      # Reserved for future use
      0::32
    >>
  end

  defp build_hint_entry(key, segment_id, offset, size, timestamp, is_tombstone) do
    key_binary = to_binary(key)
    flags = if is_tombstone, do: 1, else: 0

    <<byte_size(key_binary)::16-big, segment_id::32-big, offset::64-big, size::32-big,
      timestamp::64-big, flags::8, key_binary::binary>>
  end

  defp read_hint_header(file) do
    case :file.read(file, @hint_header_size) do
      {:ok, header} when byte_size(header) == @hint_header_size ->
        <<magic::32-big, _version::8, num_entries::32-big, _reserved::32>> = header

        if magic != @hint_magic_number do
          {:error, {:invalid_magic, magic}}
        else
          {:ok, num_entries}
        end

      {:ok, header} ->
        {:error, {:incomplete_header, byte_size(header)}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_hint_entries(_file, 0, acc), do: {:ok, Enum.reverse(acc)}

  defp read_hint_entries(file, count, acc) do
    case read_hint_entry(file) do
      {:ok, entry} ->
        read_hint_entries(file, count - 1, [entry | acc])

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_hint_entry(file) do
    # Read fixed-size part first
    # 2+4+8+4+8+1 = 27 bytes
    case :file.read(file, 27) do
      {:ok, fixed_part} when byte_size(fixed_part) == 27 ->
        <<key_len::16-big, segment_id::32-big, offset::64-big, size::32-big, timestamp::64-big,
          flags::8>> = fixed_part

        # Read key
        case :file.read(file, key_len) do
          {:ok, key_binary} when byte_size(key_binary) == key_len ->
            key = from_binary(key_binary)
            is_tombstone = (flags &&& 1) != 0

            {:ok, {key, segment_id, offset, size, timestamp, is_tombstone}}

          {:ok, key_binary} ->
            {:error, {:incomplete_key, byte_size(key_binary), key_len}}

          {:error, reason} ->
            {:error, reason}
        end

      {:ok, fixed_part} ->
        {:error, {:incomplete_entry, byte_size(fixed_part)}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp to_binary(value) when is_binary(value), do: value
  defp to_binary(value), do: :erlang.term_to_binary(value)

  defp from_binary(value) when is_binary(value) do
    # Try to deserialize as a term first, fallback to binary
    try do
      :erlang.binary_to_term(value)
    catch
      :error, :badarg -> value
    end
  end

  defp from_binary(value), do: :erlang.binary_to_term(value)
end
