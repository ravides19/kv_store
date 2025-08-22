#!/usr/bin/env elixir

# Demo script to show the new test port configuration
# This script demonstrates how ports are automatically assigned in test environment

defmodule TestPortsDemo do
  def run do
    IO.puts("🚀 KVStore Test Port Configuration Demo")
    IO.puts("=" <> String.duplicate("=", 50))
    IO.puts("")

    # Show default ports for different environments
    IO.puts("🔧 Port Configuration:")
    IO.puts("  Production/Dev HTTP Port: 8080")
    IO.puts("  Production/Dev Binary Port: 9090")
    IO.puts("  Test HTTP Port: 5050")
    IO.puts("  Test Binary Port: 6060")
    IO.puts("")

    # Test single node configuration
    test_single_node()

    # Test cluster configuration
    test_cluster_configuration()

    # Test custom base ports
    test_custom_base_ports()

    IO.puts("✅ Demo completed successfully!")
  end

  def test_single_node do
    IO.puts("🏠 Single Node Configuration:")
    IO.puts("  Clustering: Disabled")

    # Simulate test environment by setting environment variable
    System.put_env("MIX_ENV", "test")

    # Load the config module
    Code.require_file("lib/kv_store/config.ex")

    http_port = KVStore.Config.server_port()
    binary_port = KVStore.Config.binary_server_port()

    IO.puts("  HTTP Server Port: #{http_port}")
    IO.puts("  Binary Server Port: #{binary_port}")
    IO.puts("")

    # Clean up
    System.delete_env("MIX_ENV")
  end

  def test_cluster_configuration do
    IO.puts("🌐 Cluster Configuration (Test Environment):")
    IO.puts("  Clustering: Enabled")
    IO.puts("  Cluster Nodes: node1, node2, node3, node4")
    IO.puts("")

    # Simulate test environment
    System.put_env("MIX_ENV", "test")

    # Load the config module
    Code.require_file("lib/kv_store/config.ex")

    # Test each node
    ["node1", "node2", "node3", "node4"]
    |> Enum.each(fn node_id ->
      # Set environment variables
      System.put_env("KV_CLUSTER_ENABLED", "true")
      System.put_env("KV_NODE_ID", node_id)
      System.put_env("KV_CLUSTER_NODES", "node1,node2,node3,node4")

      http_port = KVStore.Config.server_port()
      binary_port = KVStore.Config.binary_server_port()

      IO.puts("  #{node_id}: HTTP #{http_port}, Binary #{binary_port}")
    end)

    IO.puts("")

    # Clean up
    System.delete_env("MIX_ENV")
    System.delete_env("KV_CLUSTER_ENABLED")
    System.delete_env("KV_NODE_ID")
    System.delete_env("KV_CLUSTER_NODES")
  end

  def test_custom_base_ports do
    IO.puts("⚙️  Custom Base Ports (Test Environment):")
    IO.puts("  Custom HTTP Base: 7000")
    IO.puts("  Custom Binary Base: 8000")
    IO.puts("")

    # Simulate test environment
    System.put_env("MIX_ENV", "test")

    # Load the config module
    Code.require_file("lib/kv_store/config.ex")

    # Test with custom base ports
    ["node1", "node2", "node3"]
    |> Enum.each(fn node_id ->
      # Set environment variables
      System.put_env("KV_CLUSTER_ENABLED", "true")
      System.put_env("KV_NODE_ID", node_id)
      System.put_env("KV_CLUSTER_NODES", "node1,node2,node3")
      System.put_env("KV_PORT", "7000")
      System.put_env("KV_BINARY_PORT", "8000")

      http_port = KVStore.Config.server_port()
      binary_port = KVStore.Config.binary_server_port()

      IO.puts("  #{node_id}: HTTP #{http_port}, Binary #{binary_port}")
    end)

    IO.puts("")

    # Clean up
    System.delete_env("MIX_ENV")
    System.delete_env("KV_CLUSTER_ENABLED")
    System.delete_env("KV_NODE_ID")
    System.delete_env("KV_CLUSTER_NODES")
    System.delete_env("KV_PORT")
    System.delete_env("KV_BINARY_PORT")
  end
end

# Run the demo
TestPortsDemo.run()
