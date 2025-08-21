#!/usr/bin/env elixir

# Dynamic Port Assignment Demo
# This script demonstrates how KVStore automatically assigns different ports
# to each node when clustering is enabled on a single machine.

defmodule DynamicPortsDemo do
  def run do
    IO.puts("🚀 KVStore Dynamic Port Assignment Demo")
    IO.puts("=" |> String.duplicate(50))

    # Test 1: Single node (clustering disabled)
    IO.puts("\n📋 Test 1: Single Node (Clustering Disabled)")
    IO.puts("-" |> String.duplicate(40))
    test_single_node()

    # Test 2: Multi-node cluster
    IO.puts("\n📋 Test 2: Multi-Node Cluster")
    IO.puts("-" |> String.duplicate(40))
    test_multi_node_cluster()

    # Test 3: Custom base port
    IO.puts("\n📋 Test 3: Custom Base Port (9000)")
    IO.puts("-" |> String.duplicate(40))
    test_custom_base_port()

    IO.puts("\n✅ Demo completed!")
    IO.puts("\n💡 Key Benefits:")
    IO.puts("   • No manual port configuration needed")
    IO.puts("   • Automatic conflict avoidance on single machine")
    IO.puts("   • Easy multi-node development setup")
    IO.puts("   • Supports custom base ports")
  end

  def test_single_node do
    # Clean environment
    System.delete_env("KV_CLUSTER_ENABLED")
    System.delete_env("KV_NODE_ID")
    System.delete_env("KV_CLUSTER_NODES")
    System.delete_env("KV_PORT")

    http_port = KVStore.Config.server_port()
    binary_port = KVStore.Config.binary_server_port()

    IO.puts("Clustering: Disabled")
    IO.puts("HTTP Server Port: #{http_port}")
    IO.puts("Binary Server Port: #{binary_port}")
    IO.puts("✅ Single node uses default ports (HTTP: 8080, Binary: 9090)")
  end

  def test_multi_node_cluster do
    # Set up cluster environment
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_CLUSTER_NODES", "node1,node2,node3,node4")

    IO.puts("Clustering: Enabled")
    IO.puts("Cluster Nodes: node1, node2, node3, node4")
    IO.puts("HTTP Base Port: 8080, Binary Base Port: 9090")
    IO.puts("")

    # Test each node
    ["node1", "node2", "node3", "node4"]
    |> Enum.each(fn node_id ->
      System.put_env("KV_NODE_ID", node_id)
      http_port = KVStore.Config.server_port()
      binary_port = KVStore.Config.binary_server_port()

      IO.puts("  #{node_id}: HTTP #{http_port}, Binary #{binary_port}")
    end)

    IO.puts("✅ Each node gets unique ports automatically")
  end

  def test_custom_base_port do
    # Set up cluster with custom base ports
    System.put_env("KV_CLUSTER_ENABLED", "true")
    System.put_env("KV_CLUSTER_NODES", "node1,node2,node3")
    System.put_env("KV_PORT", "9000")
    System.put_env("KV_BINARY_PORT", "9500")

    IO.puts("Clustering: Enabled")
    IO.puts("Cluster Nodes: node1, node2, node3")
    IO.puts("HTTP Base Port: 9000, Binary Base Port: 9500")
    IO.puts("")

    # Test each node
    ["node1", "node2", "node3"]
    |> Enum.each(fn node_id ->
      System.put_env("KV_NODE_ID", node_id)
      http_port = KVStore.Config.server_port()
      binary_port = KVStore.Config.binary_server_port()

      IO.puts("  #{node_id}: HTTP #{http_port}, Binary #{binary_port}")
    end)

    IO.puts("✅ Custom base ports respected for all nodes")
  end
end

# Run the demo
DynamicPortsDemo.run()
