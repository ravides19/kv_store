defmodule KVStore.Storage.Record do
  @moduledoc """
  Record format for storing key-value pairs on disk.

  This module handles the serialization and deserialization of records
  following the Bitcask-style format with headers and checksums.
  """

  import Bitwise

  # Record header format:
  # - Magic number (4 bytes): 0xCAFEBABE
  # - Version (1 byte): 0x01
  # - Timestamp (8 bytes): Unix timestamp in milliseconds
  # - Key length (4 bytes): Length of key in bytes
  # - Value length (8 bytes): Length of value in bytes (0 for tombstones)
  # - Flags (1 byte): Bit flags (bit 0: tombstone, bits 1-7: reserved)
  # - Checksum (4 bytes): CRC32 of header + payload
  # Total header size: 30 bytes (4+1+8+4+8+1+4)

  @magic_number 0xCAFEBABE
  @version 0x01
  @header_size 30
  @tombstone_flag 0x01

  @doc """
  Create a record for storing a key-value pair.
  """
  def create(key, value, timestamp \\ :os.system_time(:millisecond)) do
    key_binary = to_binary(key)
    value_binary = to_binary(value)

    header = build_header(key_binary, value_binary, timestamp, false)
    checksum = calculate_checksum(header, key_binary, value_binary)

    # Update checksum in header
    header_with_checksum = update_checksum(header, checksum)

    # Combine header and payload
    [header_with_checksum, key_binary, value_binary]
  end

  @doc """
  Create a tombstone record for deleting a key.
  """
  def create_tombstone(key, timestamp \\ :os.system_time(:millisecond)) do
    key_binary = to_binary(key)

    header = build_header(key_binary, "", timestamp, true)
    checksum = calculate_checksum(header, key_binary, "")

    # Update checksum in header
    header_with_checksum = update_checksum(header, checksum)

    # Combine header and key (no value for tombstones)
    [header_with_checksum, key_binary]
  end

  @doc """
  Read a record from a file at the given offset.
  Returns {:ok, record} or {:error, reason}.
  """
  def read(file, offset) do
    case read_header(file, offset) do
      {:ok, header, header_size} ->
        case parse_header(header) do
          {:ok, header_data} ->
            case read_payload(file, offset + header_size, header_data) do
              {:ok, key, value} ->
                record = %{
                  magic: header_data.magic,
                  version: header_data.version,
                  timestamp: header_data.timestamp,
                  key: key,
                  value: value,
                  is_tombstone: header_data.is_tombstone,
                  checksum: header_data.checksum,
                  total_size: header_size + header_data.key_len + header_data.value_len
                }

                {:ok, record}

              {:error, reason} ->
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:error, :eof} ->
        {:error, :eof}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Get the total size of a record (header + key + value).
  """
  def size(key, value) do
    key_binary = to_binary(key)
    value_binary = to_binary(value)
    @header_size + byte_size(key_binary) + byte_size(value_binary)
  end

  @doc """
  Get the header size (constant).
  """
  def header_size, do: @header_size

  # Private functions

  defp build_header(key_binary, value_binary, timestamp, is_tombstone) do
    flags = if is_tombstone, do: @tombstone_flag, else: 0

    # Build header without checksum (will be calculated later)
    <<
      @magic_number::32-big,
      @version::8,
      timestamp::64-big,
      byte_size(key_binary)::32-big,
      byte_size(value_binary)::64-big,
      flags::8,
      # Placeholder for checksum
      0::32
    >>
  end

  defp calculate_checksum(header, key_binary, value_binary) do
    # Calculate CRC32 of header (without checksum field) + key + value
    data = binary_part(header, 0, @header_size - 4) <> key_binary <> value_binary
    :erlang.crc32(data)
  end

  defp update_checksum(header, checksum) do
    # Replace the placeholder checksum with the actual one
    header_size_without_checksum = @header_size - 4
    binary_part(header, 0, header_size_without_checksum) <> <<checksum::32-big>>
  end

  defp read_header(file, offset) do
    case :file.pread(file, offset, @header_size) do
      {:ok, header} when byte_size(header) == @header_size ->
        {:ok, header, @header_size}

      {:ok, header} ->
        {:error, {:incomplete_header, byte_size(header)}}

      {:error, :eof} ->
        {:error, :eof}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_header(header) do
    try do
      <<magic::32-big, version::8, timestamp::64-big, key_len::32-big, value_len::64-big,
        flags::8, checksum::32-big>> = header

      if magic != @magic_number do
        {:error, {:invalid_magic, magic}}
      else
        {:ok,
         %{
           magic: magic,
           version: version,
           timestamp: timestamp,
           key_len: key_len,
           value_len: value_len,
           is_tombstone: (flags &&& @tombstone_flag) != 0,
           checksum: checksum
         }}
      end
    rescue
      _ -> {:error, :invalid_header_format}
    end
  end

  defp read_payload(file, offset, header_data) do
    total_payload_size = header_data.key_len + header_data.value_len

    case :file.pread(file, offset, total_payload_size) do
      {:ok, payload} when byte_size(payload) == total_payload_size ->
        key = binary_part(payload, 0, header_data.key_len)
        value = binary_part(payload, header_data.key_len, header_data.value_len)
        {:ok, key, value}

      {:ok, payload} ->
        {:error, {:incomplete_payload, byte_size(payload), total_payload_size}}

      {:error, :eof} ->
        {:error, :eof}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp to_binary(value) when is_binary(value), do: value
  defp to_binary(value), do: :erlang.term_to_binary(value)
end
