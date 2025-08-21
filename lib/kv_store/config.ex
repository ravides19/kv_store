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

  # Network configuration
  @default_port 8080
  @default_host "127.0.0.1"

  # Cluster configuration
  @default_cluster_enabled false
  @default_node_id "node1"
  @default_cluster_nodes ["node1", "node2", "node3"]
  @default_raft_election_timeout_ms 150
  @default_raft_heartbeat_interval_ms 50

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
  Get cache configuration.
  """
  def cache_config do
    [
      max_entries: get_env_int("KV_CACHE_MAX_ENTRIES", 1000),
      max_memory_mb: get_env_int("KV_CACHE_MAX_MEMORY_MB", 100),
      ttl_seconds: get_env_int("KV_CACHE_TTL_SECONDS", 300)
    ]
  end

  @doc """
  Get compression configuration.
  """
  def compression_config do
    [
      algorithm: get_env_atom("KV_COMPRESSION_ALGORITHM", :lz4),
      level: get_env_int("KV_COMPRESSION_LEVEL", 1),
      min_size: get_env_int("KV_COMPRESSION_MIN_SIZE", 1024)
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

  @doc """
  Get server port with dynamic assignment for cluster mode.

  When clustering is enabled and running on a single machine:
  - node1 uses port 8080
  - node2 uses port 8081
  - node3 uses port 8082
  - etc.

  When clustering is disabled, uses the default port (8080).
  """
  def server_port do
    cluster_config = cluster_config()

    if cluster_config[:enabled] do
      # Get the node ID and find its position in the cluster nodes list
      node_id = cluster_config[:node_id]
      cluster_nodes = cluster_config[:cluster_nodes]

      case Enum.find_index(cluster_nodes, &(&1 == node_id)) do
        nil ->
          # Node not found in cluster, use default port
          get_env_int("KV_PORT", @default_port)

        index ->
          # Calculate port based on node position: 8080 + index
          base_port = get_env_int("KV_PORT", @default_port)
          base_port + index
      end
    else
      # Clustering disabled, use default port
      get_env_int("KV_PORT", @default_port)
    end
  end

  @doc """
  Get server host.
  """
  def server_host do
    get_env("KV_HOST", @default_host)
  end

  @doc """
  Get binary server port with dynamic assignment for cluster mode.

  When clustering is enabled and running on a single machine:
  - node1 uses port 9090
  - node2 uses port 9091
  - node3 uses port 9092
  - etc.

  When clustering is disabled, uses the default port (9090).
  """
  def binary_server_port do
    cluster_config = cluster_config()

    if cluster_config[:enabled] do
      # Get the node ID and find its position in the cluster nodes list
      node_id = cluster_config[:node_id]
      cluster_nodes = cluster_config[:cluster_nodes]

      case Enum.find_index(cluster_nodes, &(&1 == node_id)) do
        nil ->
          # Node not found in cluster, use default binary port
          get_env_int("KV_BINARY_PORT", 9090)

        index ->
          # Calculate binary port based on node position: 9090 + index
          base_binary_port = get_env_int("KV_BINARY_PORT", 9090)
          base_binary_port + index
      end
    else
      # Clustering disabled, use default binary port
      get_env_int("KV_BINARY_PORT", 9090)
    end
  end

  @doc """
  Get cluster configuration.
  """
  def cluster_config do
    [
      enabled: get_env_bool("KV_CLUSTER_ENABLED", @default_cluster_enabled),
      node_id: get_env("KV_NODE_ID", @default_node_id),
      cluster_nodes: get_env_list("KV_CLUSTER_NODES", @default_cluster_nodes),
      election_timeout_ms:
        get_env_int("KV_RAFT_ELECTION_TIMEOUT_MS", @default_raft_election_timeout_ms),
      heartbeat_interval_ms:
        get_env_int("KV_RAFT_HEARTBEAT_INTERVAL_MS", @default_raft_heartbeat_interval_ms)
    ]
  end

  @doc """
  Get node ID.
  """
  def node_id do
    get_env("KV_NODE_ID", @default_node_id)
  end

  @doc """
  Get cluster nodes.
  """
  def cluster_nodes do
    get_env_list("KV_CLUSTER_NODES", @default_cluster_nodes)
  end

  @doc """
  Get Raft election timeout.
  """
  def raft_election_timeout_ms do
    get_env_int("KV_RAFT_ELECTION_TIMEOUT_MS", @default_raft_election_timeout_ms)
  end

  @doc """
  Get Raft heartbeat interval.
  """
  def raft_heartbeat_interval_ms do
    get_env_int("KV_RAFT_HEARTBEAT_INTERVAL_MS", @default_raft_heartbeat_interval_ms)
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

  defp get_env_list(key, default) do
    case System.get_env(key) do
      nil -> default
      value -> String.split(value, ",")
    end
  end
end
