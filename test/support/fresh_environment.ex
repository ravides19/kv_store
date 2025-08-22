defmodule KVStore.FreshEnvironment do
  @moduledoc """
  Provides fresh environment setup for tests to avoid conflicts.

  This module ensures each test file gets a clean environment with:
  - Fresh temporary directories
  - Isolated port configurations
  - Clean application state
  """

  def setup_fresh_environment do
    # Create a unique test directory for this test run
    test_dir = Path.join(System.tmp_dir!(), "kv_store_test_#{:rand.uniform(1_000_000)}")
    File.rm_rf!(test_dir)
    File.mkdir_p!(test_dir)

    # Set up environment variables for isolated testing
    env_vars = %{
      "KV_DATA_DIR" => test_dir,
      "KV_SEGMENT_MAX_BYTES" => "1024",
      "KV_CLUSTER_ENABLED" => "false",
      "KV_NODE_ID" => "test_node_#{:rand.uniform(1_000_000)}",
      "KV_CLUSTER_NODES" => "",
      "KV_PORT" => "#{5000 + :rand.uniform(1000)}",
      "KV_BINARY_PORT" => "#{6000 + :rand.uniform(1000)}"
    }

    # Set environment variables
    Enum.each(env_vars, fn {key, value} ->
      System.put_env(key, value)
    end)

    # Stop any running applications to ensure clean state
    Application.stop(:kv_store)
    Application.unload(:kv_store)

    # Don't start the application automatically - let tests start what they need

    # Clean up function to be called after tests
    cleanup_fn = fn ->
      # Stop the application
      Application.stop(:kv_store)

      # Remove test directory
      File.rm_rf!(test_dir)

      # Restore original environment variables
      Enum.each(env_vars, fn {key, _value} ->
        System.delete_env(key)
      end)
    end

    # Return the test directory and cleanup function
    {:ok, test_dir: test_dir, cleanup: cleanup_fn, env_vars: env_vars}
  end

  def setup_isolated_storage(test_dir) do
    # Ensure storage processes are stopped
    KVStore.TestHelper.stop_storage_processes()

    # Start fresh storage
    KVStore.TestHelper.setup_isolated_storage(test_dir)
  end

  def setup_isolated_server(test_dir) do
    # Start fresh server (stop function doesn't exist, so we rely on port isolation)
    KVStore.ServerTestHelper.start_isolated_server(test_dir)
  end

  def cleanup_environment(cleanup_fn) when is_function(cleanup_fn) do
    cleanup_fn.()
  end

  def cleanup_environment(_), do: :ok
end
