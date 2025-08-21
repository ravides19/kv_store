defmodule KVStore.Config do
  @moduledoc """
  Configuration module for the KV store.

  Provides centralized configuration management with sensible defaults
  and environment variable overrides.
  """

  # Storage configuration
  @default_data_dir "data"
  # 100MB
  @default_segment_max_bytes 100 * 1024 * 1024
  @default_sync_on_put true

  # File cache configuration
  @default_max_files 10

  # Compaction configuration
  @default_merge_trigger_ratio 0.3
  @default_merge_throttle_ms 10

  # Network configuration (for future phases)
  @default_port 8080
  @default_host "127.0.0.1"

  @doc """
  Get storage configuration.
  """
  def storage_config do
    [
      data_dir: get_env("KV_DATA_DIR", @default_data_dir),
      segment_max_bytes: get_env_int("KV_SEGMENT_MAX_BYTES", @default_segment_max_bytes),
      sync_on_put: get_env_bool("KV_SYNC_ON_PUT", @default_sync_on_put)
    ]
  end

  @doc """
  Get file cache configuration.
  """
  def file_cache_config do
    [
      max_files: get_env_int("KV_MAX_FILES", @default_max_files)
    ]
  end

  @doc """
  Get compaction configuration.
  """
  def compactor_config do
    [
      merge_trigger_ratio: get_env_float("KV_MERGE_TRIGGER_RATIO", @default_merge_trigger_ratio),
      merge_throttle_ms: get_env_int("KV_MERGE_THROTTLE_MS", @default_merge_throttle_ms)
    ]
  end

  @doc """
  Get durability configuration.
  """
  def durability_config do
    [
      data_dir: get_env("KV_DATA_DIR", @default_data_dir),
      checkpoint_ops: get_env_int("KV_CHECKPOINT_OPS", 1000),
      checkpoint_interval_ms: get_env_int("KV_CHECKPOINT_INTERVAL_MS", 60_000),
      sync_policy: get_env_atom("KV_WAL_SYNC_POLICY", :sync_on_write)
    ]
  end

  @doc """
  Get network configuration.
  """
  def network_config do
    [
      port: get_env_int("KV_PORT", @default_port),
      host: get_env("KV_HOST", @default_host)
    ]
  end

  # Private helper functions

  defp get_env(key, default) do
    case System.get_env(key) do
      nil -> default
      value -> value
    end
  end

  defp get_env_int(key, default) do
    case System.get_env(key) do
      nil ->
        default

      value ->
        case Integer.parse(value) do
          {int, _} -> int
          :error -> default
        end
    end
  end

  defp get_env_float(key, default) do
    case System.get_env(key) do
      nil ->
        default

      value ->
        case Float.parse(value) do
          {float, _} -> float
          :error -> default
        end
    end
  end

  defp get_env_bool(key, default) do
    case System.get_env(key) do
      nil -> default
      "true" -> true
      "false" -> false
      _ -> default
    end
  end

  defp get_env_atom(key, default) do
    case System.get_env(key) do
      nil ->
        default

      value ->
        try do
          String.to_existing_atom(value)
        catch
          :error, :badarg -> default
        end
    end
  end
end
