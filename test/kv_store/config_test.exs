defmodule KVStore.ConfigTest do
  use ExUnit.Case, async: true

  alias KVStore.Config

  setup do
    # Clean up environment variables before each test
    System.delete_env("KV_CLUSTER_ENABLED")
    System.delete_env("KV_NODE_ID")
    System.delete_env("KV_CLUSTER_NODES")
    System.delete_env("KV_PORT")
    System.delete_env("KV_BINARY_PORT")
    :ok
  end

  describe "server_port/0" do
    test "returns default port when clustering is disabled" do
      assert Config.server_port() == 8080
    end

    test "returns default port when clustering is enabled but node not in cluster" do
      System.put_env("KV_CLUSTER_ENABLED", "true")
      System.put_env("KV_NODE_ID", "unknown_node")
      System.put_env("KV_CLUSTER_NODES", "node1,node2,node3")

      assert Config.server_port() == 8080
    end

    test "returns port 8080 for node1 in cluster" do
      System.put_env("KV_CLUSTER_ENABLED", "true")
      System.put_env("KV_NODE_ID", "node1")
      System.put_env("KV_CLUSTER_NODES", "node1,node2,node3")

      assert Config.server_port() == 8080
    end

    test "returns port 8081 for node2 in cluster" do
      System.put_env("KV_CLUSTER_ENABLED", "true")
      System.put_env("KV_NODE_ID", "node2")
      System.put_env("KV_CLUSTER_NODES", "node1,node2,node3")

      assert Config.server_port() == 8081
    end

    test "returns port 8082 for node3 in cluster" do
      System.put_env("KV_CLUSTER_ENABLED", "true")
      System.put_env("KV_NODE_ID", "node3")
      System.put_env("KV_CLUSTER_NODES", "node1,node2,node3")

      assert Config.server_port() == 8082
    end

    test "respects custom base port when clustering is enabled" do
      System.put_env("KV_CLUSTER_ENABLED", "true")
      System.put_env("KV_NODE_ID", "node2")
      System.put_env("KV_CLUSTER_NODES", "node1,node2,node3")
      System.put_env("KV_PORT", "9000")

      assert Config.server_port() == 9001
    end

    test "handles different cluster node orders" do
      System.put_env("KV_CLUSTER_ENABLED", "true")
      System.put_env("KV_NODE_ID", "node2")
      System.put_env("KV_CLUSTER_NODES", "node3,node2,node1")

      # node2 is at index 1, so should get port 8081 (8080 + 1)
      assert Config.server_port() == 8081
    end
  end

  describe "binary_server_port/0" do
    test "returns default binary port when clustering is disabled" do
      assert Config.binary_server_port() == 9090
    end

    test "returns correct binary port for node1 in cluster" do
      System.put_env("KV_CLUSTER_ENABLED", "true")
      System.put_env("KV_NODE_ID", "node1")
      System.put_env("KV_CLUSTER_NODES", "node1,node2,node3")

      assert Config.binary_server_port() == 9090
    end

    test "returns correct binary port for node2 in cluster" do
      System.put_env("KV_CLUSTER_ENABLED", "true")
      System.put_env("KV_NODE_ID", "node2")
      System.put_env("KV_CLUSTER_NODES", "node1,node2,node3")

      assert Config.binary_server_port() == 9091
    end

    test "returns correct binary port for node3 in cluster" do
      System.put_env("KV_CLUSTER_ENABLED", "true")
      System.put_env("KV_NODE_ID", "node3")
      System.put_env("KV_CLUSTER_NODES", "node1,node2,node3")

      assert Config.binary_server_port() == 9092
    end

    test "respects custom binary base port when clustering is enabled" do
      System.put_env("KV_CLUSTER_ENABLED", "true")
      System.put_env("KV_NODE_ID", "node2")
      System.put_env("KV_CLUSTER_NODES", "node1,node2,node3")
      System.put_env("KV_BINARY_PORT", "9500")

      # 9500 + 1 (node2 is at index 1)
      assert Config.binary_server_port() == 9501
    end
  end

  describe "cluster_config/0" do
    test "returns default cluster configuration" do
      config = Config.cluster_config()

      assert config[:enabled] == false
      assert config[:node_id] == "node1"
      assert config[:cluster_nodes] == ["node1", "node2", "node3"]
    end

    test "respects environment variable overrides" do
      System.put_env("KV_CLUSTER_ENABLED", "true")
      System.put_env("KV_NODE_ID", "my_node")
      System.put_env("KV_CLUSTER_NODES", "node_a,node_b,node_c")

      config = Config.cluster_config()

      assert config[:enabled] == true
      assert config[:node_id] == "my_node"
      assert config[:cluster_nodes] == ["node_a", "node_b", "node_c"]
    end
  end
end
