defmodule KVStore.DynamicPortTest do
  use ExUnit.Case, async: false

  setup do
    # Clean up environment variables
    System.delete_env("KV_CLUSTER_ENABLED")
    System.delete_env("KV_NODE_ID")
    System.delete_env("KV_CLUSTER_NODES")
    System.delete_env("KV_PORT")
    System.delete_env("KV_BINARY_PORT")

    # Clean up any existing data
    File.rm_rf("data")
    :ok
  end

  test "single node uses default ports when clustering is disabled" do
    # Clustering disabled (default)
    assert KVStore.Config.server_port() == 8080
    assert KVStore.Config.binary_server_port() == 9090
  end

  test "node1 uses ports 8080/9090 when clustering is enabled" do
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "node1")
    System.put_env("KV_CLUSTER_NODES", "node1,node2,node3")

    assert KVStore.Config.server_port() == 8080
    assert KVStore.Config.binary_server_port() == 9090
  end

  test "node2 uses ports 8081/9091 when clustering is enabled" do
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "node2")
    System.put_env("KV_CLUSTER_NODES", "node1,node2,node3")

    assert KVStore.Config.server_port() == 8081
    assert KVStore.Config.binary_server_port() == 9091
  end

  test "node3 uses ports 8082/9092 when clustering is enabled" do
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "node3")
    System.put_env("KV_CLUSTER_NODES", "node1,node2,node3")

    assert KVStore.Config.server_port() == 8082
    assert KVStore.Config.binary_server_port() == 9092
  end

  test "respects custom base port when clustering is enabled" do
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "node2")
    System.put_env("KV_CLUSTER_NODES", "node1,node2,node3")
    System.put_env("KV_PORT", "9000")
    System.put_env("KV_BINARY_PORT", "9500")

    # 9000 + 1 (node2 is at index 1)
    assert KVStore.Config.server_port() == 9001
    # 9500 + 1 (node2 is at index 1)
    assert KVStore.Config.binary_server_port() == 9501
  end

  test "handles different cluster node orders correctly" do
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "node2")
    System.put_env("KV_CLUSTER_NODES", "node3,node2,node1")

    # node2 is at index 1 in the list ["node3", "node2", "node1"]
    # 8080 + 1
    assert KVStore.Config.server_port() == 8081
    # 9090 + 1
    assert KVStore.Config.binary_server_port() == 9091
  end

  test "falls back to default port when node not found in cluster" do
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_NODE_ID", "unknown_node")
    System.put_env("KV_CLUSTER_NODES", "node1,node2,node3")

    assert KVStore.Config.server_port() == 8080
    assert KVStore.Config.binary_server_port() == 9090
  end
end
