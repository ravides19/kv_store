defmodule KVStore.ServerTestHelper do
  @moduledoc """
  Test helper for server tests that provides isolated HTTP server testing.
  """

  @doc """
  Start an isolated HTTP server for testing.
  Returns {server_pid, cleanup_fn} for use in test setup.
  """
  def start_isolated_server(opts \\ []) do
    # Create temporary test directory
    test_dir = Path.join(System.tmp_dir!(), "kv_store_server_test_#{:rand.uniform(1_000_000)}")
    File.mkdir_p!(test_dir)

    # Store original environment
    original_env = %{
      data_dir: System.get_env("KV_DATA_DIR"),
      cluster_enabled: System.get_env("KV_CLUSTER_ENABLED"),
      node_id: System.get_env("KV_NODE_ID"),
      cluster_nodes: System.get_env("KV_CLUSTER_NODES"),
      port: System.get_env("KV_PORT")
    }

    # Set test environment
    System.put_env("KV_DATA_DIR", test_dir)
    # Disable clustering for server tests
    System.put_env("KV_CLUSTER_ENABLED", "false")
    System.delete_env("KV_NODE_ID")
    System.delete_env("KV_CLUSTER_NODES")

    # Stop any existing processes
    stop_all_processes()

    # Wait for processes to stop
    Process.sleep(100)

    # Start only storage components (not the full application)
    KVStore.TestHelper.start_storage_only()

    # Wait for storage to initialize
    Process.sleep(100)

    # Start HTTP server on a random port to avoid conflicts
    server_port = Keyword.get(opts, :port, :rand.uniform(1000) + 5050)
    System.put_env("KV_PORT", to_string(server_port))

    # Start test HTTP server using Plug.Cowboy
    case Plug.Cowboy.http(KVStore.TestServer, [], port: server_port) do
      {:ok, server_pid} ->
        # Wait for server to start
        Process.sleep(100)

        cleanup_fn = fn ->
          # Stop server
          try do
            Plug.Cowboy.shutdown(KVStore.TestServer.HTTP)
          catch
            :exit, _ -> :ok
          end

          # Stop storage with timeout
          try do
            GenServer.stop(KVStore.Storage.Supervisor, :normal, 2000)
          catch
            :exit, _ -> :ok
          end

          # Wait for cleanup
          Process.sleep(100)

          # Restore environment
          restore_environment(original_env)

          # Clean up test directory
          File.rm_rf(test_dir)
        end

        {server_pid, server_port, cleanup_fn}

      {:error, reason} ->
        # Clean up on failure
        File.rm_rf(test_dir)
        restore_environment(original_env)
        raise "Failed to start test server: #{inspect(reason)}"
    end
  end

  @doc """
  Stop all KVStore processes.
  """
  def stop_all_processes do
    processes = [
      KVStore.Server,
      KVStore.BinaryServer,
      KVStore.Cluster.Manager,
      KVStore.Storage.Supervisor,
      KVStore.Storage.Engine,
      KVStore.Storage.FileCache,
      KVStore.Storage.Compactor,
      KVStore.Storage.Durability,
      KVStore.Storage.Cache
    ]

    Enum.each(processes, fn process_name ->
      case Process.whereis(process_name) do
        nil ->
          :ok

        pid ->
          try do
            # Use a timeout to avoid hanging
            GenServer.stop(pid, :normal, 1000)
          catch
            :exit, _ -> :ok
          end
      end
    end)
  end

  @doc """
  Restore original environment variables.
  """
  def restore_environment(original_env) do
    if original_env.data_dir do
      System.put_env("KV_DATA_DIR", original_env.data_dir)
    else
      System.delete_env("KV_DATA_DIR")
    end

    if original_env.cluster_enabled do
      System.put_env("KV_CLUSTER_ENABLED", original_env.cluster_enabled)
    else
      System.delete_env("KV_CLUSTER_ENABLED")
    end

    if original_env.node_id do
      System.put_env("KV_NODE_ID", original_env.node_id)
    else
      System.delete_env("KV_NODE_ID")
    end

    if original_env.cluster_nodes do
      System.put_env("KV_CLUSTER_NODES", original_env.cluster_nodes)
    else
      System.delete_env("KV_CLUSTER_NODES")
    end

    if original_env.port do
      System.put_env("KV_PORT", original_env.port)
    else
      System.delete_env("KV_PORT")
    end
  end

  @doc """
  Make an HTTP request to the test server.
  """
  def http_request(method, path, body \\ nil, headers \\ []) do
    server_port = KVStore.Config.server_port()
    url = "http://localhost:#{server_port}#{path}"

    case method do
      :get ->
        HTTPoison.get(url, headers)

      :put ->
        HTTPoison.put(url, body, [{"Content-Type", "application/json"} | headers])

      :delete ->
        HTTPoison.delete(url, headers)

      :post ->
        HTTPoison.post(url, body, [{"Content-Type", "application/json"} | headers])
    end
  end
end
