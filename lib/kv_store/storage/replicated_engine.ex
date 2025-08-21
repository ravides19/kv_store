defmodule KVStore.Storage.ReplicatedEngine do
  @moduledoc """
  Replicated storage engine that coordinates with the cluster for distributed operations.

  This module wraps the existing storage engine and adds cluster coordination
  for write operations while allowing read operations to be served locally.
  """

  @doc """
  Put a key-value pair with cluster replication.
  """
  def put(key, value) do
    command = %{
      operation: :put,
      key: key,
      value: value
    }

    case KVStore.Cluster.Manager.submit_command(command) do
      {:ok, _result} ->
        {:ok, :replicated}

      {:error, :cluster_disabled} ->
        # Fall back to local storage
        KVStore.Storage.Engine.put(key, value)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Get a value (served locally for performance).
  """
  def get(key) do
    # Reads are served locally for performance
    # In a production system, you might want to check consistency levels
    KVStore.Storage.Engine.get(key)
  end

  @doc """
  Delete a key with cluster replication.
  """
  def delete(key) do
    command = %{
      operation: :delete,
      key: key
    }

    case KVStore.Cluster.Manager.submit_command(command) do
      {:ok, _result} ->
        {:ok, :replicated}

      {:error, :cluster_disabled} ->
        # Fall back to local storage
        KVStore.Storage.Engine.delete(key)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Get a range of keys (served locally for performance).
  """
  def range(start_key, end_key) do
    # Reads are served locally for performance
    KVStore.Storage.Engine.range(start_key, end_key)
  end

  @doc """
  Batch put operations with cluster replication.
  """
  def batch_put(kv_pairs) do
    command = %{
      operation: :batch_put,
      kv_pairs: kv_pairs
    }

    case KVStore.Cluster.Manager.submit_command(command) do
      {:ok, _result} ->
        {:ok, :replicated}

      {:error, :cluster_disabled} ->
        # Fall back to local storage
        KVStore.Storage.Engine.batch_put(kv_pairs)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Get storage status.
  """
  def status do
    local_status = KVStore.Storage.Engine.status()
    cluster_status = KVStore.Cluster.Manager.cluster_status()

    Map.merge(local_status, %{
      cluster: cluster_status
    })
  end

  @doc """
  Get cluster status.
  """
  def cluster_status do
    KVStore.Cluster.Manager.cluster_status()
  end

  @doc """
  Get the current leader.
  """
  def get_leader do
    KVStore.Cluster.Manager.get_leader()
  end
end
