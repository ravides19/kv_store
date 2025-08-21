defmodule KVStore.Storage.ReplicatedEngineTest do
  use ExUnit.Case
  require Logger

  setup do
    # Clean up any existing data
    File.rm_rf!("data")

    # Stop any existing application
    try do
      Application.stop(:kv_store)
    catch
      :exit, _ -> :ok
    end

    # Wait for processes to stop
    Process.sleep(100)

    # Start the application
    KVStore.start()

    on_exit(fn ->
      try do
        Application.stop(:kv_store)
      catch
        :exit, _ -> :ok
      end

      Process.sleep(50)
      File.rm_rf!("data")
    end)

    :ok
  end

  test "replicated engine falls back to local storage when clustering is disabled" do
    # Ensure clustering is disabled
    System.put_env("KV_CLUSTER_ENABLED", "false")

    # Restart the application
    Application.stop(:kv_store)
    Process.sleep(100)
    KVStore.start()

    # Test put operation
    result = KVStore.Storage.ReplicatedEngine.put("test_key", "test_value")
    assert {:ok, _} = result

    # Test get operation
    result = KVStore.Storage.ReplicatedEngine.get("test_key")
    assert {:ok, "test_value"} = result

    # Test delete operation
    result = KVStore.Storage.ReplicatedEngine.delete("test_key")
    assert {:ok, _} = result

    # Verify deletion
    result = KVStore.Storage.ReplicatedEngine.get("test_key")
    assert {:error, :not_found} = result
  end

  test "replicated engine uses cluster when clustering is enabled" do
    # Enable clustering
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "replicated_node")
    System.put_env("KV_CLUSTER_NODES", "replicated_node")

    # Restart the application
    Application.stop(:kv_store)
    Process.sleep(100)
    KVStore.start()

    # Wait for election
    Process.sleep(200)

    # Test put operation
    result = KVStore.Storage.ReplicatedEngine.put("test_key", "test_value")
    assert {:ok, :replicated} = result

    # Test get operation (served locally)
    result = KVStore.Storage.ReplicatedEngine.get("test_key")
    assert {:ok, "test_value"} = result

    # Test delete operation
    result = KVStore.Storage.ReplicatedEngine.delete("test_key")
    assert {:ok, :replicated} = result

    # Verify deletion
    result = KVStore.Storage.ReplicatedEngine.get("test_key")
    assert {:error, :not_found} = result
  end

  test "replicated engine handles range operations" do
    # Enable clustering
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "range_node")
    System.put_env("KV_CLUSTER_NODES", "range_node")

    # Restart the application
    Application.stop(:kv_store)
    Process.sleep(100)
    KVStore.start()

    # Wait for election
    Process.sleep(200)

    # Put some values
    {:ok, _} = KVStore.Storage.ReplicatedEngine.put("a_key", "a_value")
    {:ok, _} = KVStore.Storage.ReplicatedEngine.put("b_key", "b_value")
    {:ok, _} = KVStore.Storage.ReplicatedEngine.put("c_key", "c_value")

    # Test range operation
    result = KVStore.Storage.ReplicatedEngine.range("a_key", "c_key")
    assert {:ok, results} = result
    assert length(results) >= 3
  end

  test "replicated engine handles batch operations" do
    # Enable clustering
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "batch_node")
    System.put_env("KV_CLUSTER_NODES", "batch_node")

    # Restart the application
    Application.stop(:kv_store)
    Process.sleep(100)
    KVStore.start()

    # Wait for election
    Process.sleep(200)

    # Test batch put operation
    kv_pairs = [
      {"batch_key1", "batch_value1"},
      {"batch_key2", "batch_value2"},
      {"batch_key3", "batch_value3"}
    ]

    result = KVStore.Storage.ReplicatedEngine.batch_put(kv_pairs)
    assert {:ok, :replicated} = result

    # Verify all values were stored
    Enum.each(kv_pairs, fn {key, value} ->
      result = KVStore.Storage.ReplicatedEngine.get(key)
      assert {:ok, ^value} = result
    end)
  end

  test "replicated engine provides status information" do
    # Enable clustering
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "status_node")
    System.put_env("KV_CLUSTER_NODES", "status_node")

    # Restart the application
    Application.stop(:kv_store)
    Process.sleep(100)
    KVStore.start()

    # Wait for election
    Process.sleep(200)

    # Get status
    status = KVStore.Storage.ReplicatedEngine.status()

    assert Map.has_key?(status, :storage)
    assert Map.has_key?(status, :cluster)
    assert status.cluster.enabled == true
    assert status.cluster.node_id == "status_node"
  end

  test "replicated engine provides cluster status" do
    # Enable clustering
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "cluster_status_node")
    System.put_env("KV_CLUSTER_NODES", "cluster_status_node")

    # Restart the application
    Application.stop(:kv_store)
    Process.sleep(100)
    KVStore.start()

    # Wait for election
    Process.sleep(200)

    # Get cluster status
    status = KVStore.Storage.ReplicatedEngine.cluster_status()

    assert status.enabled == true
    assert status.node_id == "cluster_status_node"
    assert status.state in [:follower, :leader, :candidate]
  end

  test "replicated engine can get leader information" do
    # Enable clustering
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "leader_node")
    System.put_env("KV_CLUSTER_NODES", "leader_node")

    # Restart the application
    Application.stop(:kv_store)
    Process.sleep(100)
    KVStore.start()

    # Wait for election
    Process.sleep(200)

    # Get leader
    leader = KVStore.Storage.ReplicatedEngine.get_leader()
    assert leader == "leader_node"
  end

  test "replicated engine handles cluster errors gracefully" do
    # Enable clustering
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "error_node")
    System.put_env("KV_CLUSTER_NODES", "error_node")

    # Restart the application
    Application.stop(:kv_store)
    Process.sleep(100)
    KVStore.start()

    # Wait for election
    Process.sleep(200)

    # Kill the cluster manager to simulate cluster failure
    pid = Process.whereis(KVStore.Cluster.Manager)
    Process.exit(pid, :kill)

    # Operations should still work by falling back to local storage
    result = KVStore.Storage.ReplicatedEngine.put("test_key", "test_value")
    assert {:ok, _} = result

    result = KVStore.Storage.ReplicatedEngine.get("test_key")
    assert {:ok, "test_value"} = result
  end
end
