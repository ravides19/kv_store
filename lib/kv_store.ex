defmodule KVStore do
  @moduledoc """
  KVStore - A persistent key-value storage system.

  This module provides the main API for interacting with the KV store.
  The storage engine is implemented using a Bitcask-style append-only log
  with an in-memory index for fast lookups.

  ## Features (Phase 0 - Basic Structure)

  - OTP application structure with supervision
  - Storage engine with ETS-based index
  - File cache for managing open file handles
  - Background compaction process (structure only)
  - Configuration management with environment variables

  ## Usage

      # Start the application
      KVStore.start()

      # Basic operations (not yet implemented in Phase 0)
      KVStore.put("key", "value")
      KVStore.get("key")
      KVStore.delete("key")
      KVStore.range("start", "end")
      KVStore.batch_put([{"key1", "value1"}, {"key2", "value2"}])
  """

  @doc """
  Start the KV store application.
  """
  def start do
    Application.ensure_all_started(:kv_store)
  end

  @doc """
  Stop the KV store application.
  """
  def stop do
    Application.stop(:kv_store)
  end

  @doc """
  Put a key-value pair into the store.
  """
  def put(key, value) do
    if KVStore.Config.cluster_config()[:enabled] do
      KVStore.Storage.ReplicatedEngine.put(key, value)
    else
      KVStore.Storage.Engine.put(key, value)
    end
  end

  @doc """
  Get a value by key.
  """
  def get(key) do
    if KVStore.Config.cluster_config()[:enabled] do
      KVStore.Storage.ReplicatedEngine.get(key)
    else
      KVStore.Storage.Engine.get(key)
    end
  end

  @doc """
  Delete a key from the store.
  """
  def delete(key) do
    if KVStore.Config.cluster_config()[:enabled] do
      KVStore.Storage.ReplicatedEngine.delete(key)
    else
      KVStore.Storage.Engine.delete(key)
    end
  end

  @doc """
  Get a range of keys.
  """
  def range(start_key, end_key) do
    if KVStore.Config.cluster_config()[:enabled] do
      KVStore.Storage.ReplicatedEngine.range(start_key, end_key)
    else
      KVStore.Storage.Engine.range(start_key, end_key)
    end
  end

  @doc """
  Batch put multiple key-value pairs.
  """
  def batch_put(kv_pairs) do
    if KVStore.Config.cluster_config()[:enabled] do
      KVStore.Storage.ReplicatedEngine.batch_put(kv_pairs)
    else
      KVStore.Storage.Engine.batch_put(kv_pairs)
    end
  end

  @doc """
  Get storage engine status.
  """
  def status do
    if KVStore.Config.cluster_config()[:enabled] do
      KVStore.Storage.ReplicatedEngine.status()
    else
      %{
        storage: KVStore.Storage.Engine.status(),
        compactor: KVStore.Storage.Compactor.status()
      }
    end
  end

  @doc """
  Get cluster status.
  """
  def cluster_status do
    if KVStore.Config.cluster_config()[:enabled] do
      KVStore.Cluster.Manager.cluster_status()
    else
      %{enabled: false}
    end
  end

  @doc """
  Get the current leader.
  """
  def get_leader do
    if KVStore.Config.cluster_config()[:enabled] do
      KVStore.Cluster.Manager.get_leader()
    else
      nil
    end
  end
end
