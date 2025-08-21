defmodule KVStore.Cluster.ManagerTest do
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

    # Set different ports for each test to avoid conflicts
    test_port = :rand.uniform(1000) + 9000
    System.put_env("KV_PORT", to_string(test_port))

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

  test "cluster manager starts when clustering is enabled" do
    # Enable clustering via environment
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "test_node")
    System.put_env("KV_CLUSTER_NODES", "test_node")

    # Restart the application to pick up new config
    Application.stop(:kv_store)
    Process.sleep(100)
    KVStore.start()

    # Check that cluster manager is running
    status = KVStore.Cluster.Manager.cluster_status()
    assert status.enabled == true
    assert status.node_id == "test_node"
  end

  test "cluster manager is disabled by default" do
    # Ensure clustering is disabled
    System.put_env("KV_CLUSTER_ENABLED", "false")

    # Restart the application to pick up new config
    Application.stop(:kv_store)
    Process.sleep(100)
    KVStore.start()

    # Check that cluster manager is disabled
    status = KVStore.Cluster.Manager.cluster_status()
    assert status.enabled == false
  end

  test "cluster manager can submit commands" do
    # Enable clustering
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "command_node")
    System.put_env("KV_CLUSTER_NODES", "command_node")

    # Restart the application
    Application.stop(:kv_store)
    Process.sleep(100)
    KVStore.start()

    # Wait for election
    Process.sleep(200)

    # Submit a command
    command = %{operation: :put, key: "test_key", value: "test_value"}
    result = KVStore.Cluster.Manager.submit_command(command)

    assert {:ok, _} = result
  end

  test "cluster manager returns error when clustering is disabled" do
    # Ensure clustering is disabled
    System.put_env("KV_CLUSTER_ENABLED", "false")

    # Restart the application
    Application.stop(:kv_store)
    Process.sleep(100)
    KVStore.start()

    # Try to submit a command
    command = %{operation: :put, key: "test_key", value: "test_value"}
    result = KVStore.Cluster.Manager.submit_command(command)

    assert {:error, :cluster_disabled} = result
  end

  test "cluster manager can get leader information" do
    # Enable clustering
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "leader_test_node")
    System.put_env("KV_CLUSTER_NODES", "leader_test_node")

    # Restart the application
    Application.stop(:kv_store)
    Process.sleep(100)
    KVStore.start()

    # Wait for election
    Process.sleep(200)

    # Get leader
    leader = KVStore.Cluster.Manager.get_leader()
    assert leader == "leader_test_node"
  end

  test "cluster manager returns nil leader when clustering is disabled" do
    # Ensure clustering is disabled
    System.put_env("KV_CLUSTER_ENABLED", "false")

    # Restart the application
    Application.stop(:kv_store)
    Process.sleep(100)
    KVStore.start()

    # Get leader
    leader = KVStore.Cluster.Manager.get_leader()
    assert leader == nil
  end

  test "cluster manager provides cluster status" do
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

    # Get cluster status
    status = KVStore.Cluster.Manager.cluster_status()

    assert status.enabled == true
    assert status.node_id == "status_node"
    assert status.state in [:follower, :leader, :candidate]
    assert status.term >= 0
    assert status.log_length >= 0
    assert status.commit_index >= 0
    assert status.cluster_nodes == ["status_node"]
  end

  test "cluster manager handles node management operations" do
    # Enable clustering
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "management_node")
    System.put_env("KV_CLUSTER_NODES", "management_node")

    # Restart the application
    Application.stop(:kv_store)
    Process.sleep(100)
    KVStore.start()

    # Try to add a node (should return not implemented for now)
    result = KVStore.Cluster.Manager.add_node("new_node")
    assert {:error, :not_implemented} = result

    # Try to remove a node (should return not implemented for now)
    result = KVStore.Cluster.Manager.remove_node("management_node")
    assert {:error, :not_implemented} = result
  end

  test "cluster manager handles node management when disabled" do
    # Ensure clustering is disabled
    System.put_env("KV_CLUSTER_ENABLED", "false")

    # Restart the application
    Application.stop(:kv_store)
    Process.sleep(100)
    KVStore.start()

    # Try to add a node
    result = KVStore.Cluster.Manager.add_node("new_node")
    assert {:error, :cluster_disabled} = result

    # Try to remove a node
    result = KVStore.Cluster.Manager.remove_node("existing_node")
    assert {:error, :cluster_disabled} = result
  end
end
