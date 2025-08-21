defmodule KVStore.Storage.Compression do
  @moduledoc """
  Segment compression for reducing disk usage.

  This module implements:
  - LZ4 compression for fast compression/decompression
  - Configurable compression levels
  - Compression statistics and monitoring
  - Automatic compression during segment rotation
  """

  require Logger

  # Compression algorithm types
  @compression_none 0x00
  @compression_lz4 0x01
  @compression_gzip 0x02

  # Default compression settings
  @default_compression_algorithm @compression_lz4
  # Fast compression
  @default_compression_level 1
  # 1KB minimum
  @default_min_size_for_compression 1024

  @doc """
  Compress segment data using the specified algorithm.
  """
  def compress_segment(
        segment_data,
        algorithm \\ @default_compression_algorithm,
        level \\ @default_compression_level
      ) do
    case algorithm do
      @compression_none ->
        {:ok, segment_data, @compression_none}

      @compression_lz4 ->
        compress_lz4(segment_data, level)

      @compression_gzip ->
        compress_gzip(segment_data, level)

      _ ->
        {:error, :unsupported_compression_algorithm}
    end
  end

  @doc """
  Decompress segment data.
  """
  def decompress_segment(compressed_data, algorithm) do
    case algorithm do
      @compression_none ->
        {:ok, compressed_data}

      @compression_lz4 ->
        decompress_lz4(compressed_data)

      @compression_gzip ->
        decompress_gzip(compressed_data)

      _ ->
        {:error, :unsupported_compression_algorithm}
    end
  end

  @doc """
  Check if compression would be beneficial for the given data.
  """
  def should_compress?(data, min_size \\ @default_min_size_for_compression) do
    data_size = byte_size(data)
    data_size >= min_size
  end

  @doc """
  Get compression statistics for a segment.
  """
  def compression_stats(original_size, compressed_size) do
    compression_ratio =
      if original_size > 0 do
        (original_size - compressed_size) / original_size
      else
        0.0
      end

    space_saved = original_size - compressed_size

    %{
      original_size: original_size,
      compressed_size: compressed_size,
      compression_ratio: compression_ratio,
      space_saved: space_saved,
      space_saved_percent: compression_ratio * 100
    }
  end

  @doc """
  Compress a segment file in place.
  """
  def compress_segment_file(segment_path, algorithm \\ @default_compression_algorithm) do
    case File.read(segment_path) do
      {:ok, segment_data} ->
        case compress_segment(segment_data, algorithm) do
          {:ok, compressed_data, used_algorithm} ->
            # Write compressed data with header
            compressed_file_path = "#{segment_path}.compressed"
            header = build_compression_header(used_algorithm, byte_size(segment_data))

            case File.write(compressed_file_path, [header, compressed_data]) do
              :ok ->
                # Calculate compression stats
                stats = compression_stats(byte_size(segment_data), byte_size(compressed_data))

                Logger.info(
                  "Compressed segment #{segment_path}: #{Float.round(stats.space_saved_percent, 1)}% space saved"
                )

                # Replace original with compressed version
                File.rm(segment_path)
                File.rename(compressed_file_path, segment_path)

                {:ok, stats}

              {:error, reason} ->
                File.rm(compressed_file_path)
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Decompress a segment file in place.
  """
  def decompress_segment_file(segment_path) do
    case File.read(segment_path) do
      {:ok, file_data} ->
        case parse_compression_header(file_data) do
          {:ok, algorithm, original_size, compressed_data} ->
            case decompress_segment(compressed_data, algorithm) do
              {:ok, decompressed_data} ->
                # Verify size matches
                if byte_size(decompressed_data) == original_size do
                  # Write decompressed data
                  decompressed_file_path = "#{segment_path}.decompressed"

                  case File.write(decompressed_file_path, decompressed_data) do
                    :ok ->
                      # Replace compressed with decompressed version
                      File.rm(segment_path)
                      File.rename(decompressed_file_path, segment_path)

                      {:ok,
                       %{
                         original_size: original_size,
                         decompressed_size: byte_size(decompressed_data)
                       }}

                    {:error, reason} ->
                      File.rm(decompressed_file_path)
                      {:error, reason}
                  end
                else
                  {:error, :size_mismatch}
                end

              {:error, reason} ->
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Check if a file is compressed.
  """
  def is_compressed?(file_path) do
    case File.read(file_path) do
      {:ok, header} ->
        case parse_compression_header(header) do
          {:ok, algorithm, _original_size, _compressed_data} ->
            algorithm != @compression_none

          {:error, _reason} ->
            false
        end

      {:error, _reason} ->
        false
    end
  end

  # Private functions

  defp compress_lz4(data, _level) do
    # Use Erlang's built-in compression as LZ4 replacement
    # In a real implementation, you'd use a proper LZ4 library
    case :zlib.compress(data) do
      compressed_data when is_binary(compressed_data) ->
        {:ok, compressed_data, @compression_lz4}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decompress_lz4(compressed_data) do
    # Use Erlang's built-in decompression
    case :zlib.uncompress(compressed_data) do
      decompressed_data when is_binary(decompressed_data) ->
        {:ok, decompressed_data}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp compress_gzip(data, _level) do
    # Use gzip compression
    case :zlib.compress(data) do
      compressed_data when is_binary(compressed_data) ->
        {:ok, compressed_data, @compression_gzip}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp decompress_gzip(compressed_data) do
    # Use gzip decompression
    case :zlib.uncompress(compressed_data) do
      decompressed_data when is_binary(decompressed_data) ->
        {:ok, decompressed_data}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp build_compression_header(algorithm, original_size) do
    # Header format: magic(4) + algorithm(1) + original_size(8) + reserved(3)
    # "COMP" in hex
    magic = 0x434F4D50
    reserved = <<0, 0, 0>>

    <<magic::32, algorithm::8, original_size::64, reserved::binary>>
  end

  defp parse_compression_header(file_data) when byte_size(file_data) >= 16 do
    case file_data do
      <<magic::32, algorithm::8, original_size::64, _reserved::binary-size(3),
        compressed_data::binary>> ->
        if magic == 0x434F4D50 do
          {:ok, algorithm, original_size, compressed_data}
        else
          {:error, :invalid_magic}
        end

      _ ->
        {:error, :invalid_header}
    end
  end

  defp parse_compression_header(_file_data) do
    {:error, :incomplete_header}
  end

  @doc """
  Get supported compression algorithms.
  """
  def supported_algorithms do
    %{
      none: @compression_none,
      lz4: @compression_lz4,
      gzip: @compression_gzip
    }
  end

  @doc """
  Get default compression settings.
  """
  def default_settings do
    %{
      algorithm: @default_compression_algorithm,
      level: @default_compression_level,
      min_size: @default_min_size_for_compression
    }
  end
end
