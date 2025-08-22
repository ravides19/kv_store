defmodule KVStore.TestHelper do
  @moduledoc """
  Test helper functions for KVStore tests.

  This module provides utilities to start only the storage components
  without the HTTP server to avoid port conflicts during testing.
  """

  @doc """
  Start only the storage components for testing.
  This avoids starting the HTTP server which can cause port conflicts.
  """
  def start_storage_only do
    # Check if storage supervisor is already running
    case Process.whereis(KVStore.Storage.Supervisor) do
      nil ->
        # Start the storage supervisor directly with proper configuration
        case KVStore.Storage.Supervisor.start_link([]) do
          {:ok, _pid} ->
            # Wait a bit for all processes to initialize
            Process.sleep(50)

            # Verify all critical processes are running
            ensure_storage_processes_running()

            :ok

          {:error, {:already_started, _pid}} ->
            # Storage is already running, just verify processes
            ensure_storage_processes_running()
            :ok

          {:error, reason} ->
            raise "Failed to start storage supervisor: #{inspect(reason)}"
        end

      _pid ->
        # Storage is already running, just verify processes
        ensure_storage_processes_running()
        :ok
    end
  end

  @doc """
  Start storage components with custom configuration.
  Useful for tests that need specific settings.
  """
  def start_storage_with_config(opts \\ []) do
    data_dir = Keyword.get(opts, :data_dir, create_temp_test_dir())
    segment_max_bytes = Keyword.get(opts, :segment_max_bytes, 100)

    # Set environment variables for this test
    original_data_dir = System.get_env("KV_DATA_DIR")
    original_segment_max = System.get_env("KV_SEGMENT_MAX_BYTES")

    System.put_env("KV_DATA_DIR", data_dir)
    System.put_env("KV_SEGMENT_MAX_BYTES", to_string(segment_max_bytes))

    # Start storage components
    {:ok, _pid} = KVStore.Storage.Supervisor.start_link([])

    # Wait for initialization
    Process.sleep(10)

    # Verify all processes are running
    ensure_storage_processes_running()

    # Return cleanup function
    cleanup_fn = fn ->
      try do
        Supervisor.stop(KVStore.Storage.Supervisor)
      catch
        :exit, _ -> :ok
      end

      # Restore original environment
      if original_data_dir do
        System.put_env("KV_DATA_DIR", original_data_dir)
      else
        System.delete_env("KV_DATA_DIR")
      end

      if original_segment_max do
        System.put_env("KV_SEGMENT_MAX_BYTES", original_segment_max)
      else
        System.delete_env("KV_SEGMENT_MAX_BYTES")
      end

      # Clean up test directory
      File.rm_rf(data_dir)
    end

    {data_dir, cleanup_fn}
  end

  @doc """
  Ensure all storage processes are running.
  """
  def ensure_storage_processes_running do
    required_processes = [
      KVStore.Storage.Supervisor,
      KVStore.Storage.Engine,
      KVStore.Storage.FileCache,
      KVStore.Storage.Compactor,
      KVStore.Storage.Durability,
      KVStore.Storage.Cache
    ]

    # Wait up to 2 seconds for all processes to be running
    wait_for_processes(required_processes, 20, 100)
  end

  defp wait_for_processes(processes, retries, sleep_ms) when retries > 0 do
    not_running =
      Enum.filter(processes, fn process_name ->
        Process.whereis(process_name) == nil
      end)

    if Enum.empty?(not_running) do
      :ok
    else
      Process.sleep(sleep_ms)
      wait_for_processes(processes, retries - 1, sleep_ms)
    end
  end

  defp wait_for_processes(processes, 0, _sleep_ms) do
    not_running =
      Enum.filter(processes, fn process_name ->
        Process.whereis(process_name) == nil
      end)

    if not Enum.empty?(not_running) do
      raise "Required processes are not running: #{inspect(not_running)}"
    end

    :ok
  end

  @doc """
  Stop the storage components.
  """
  def stop_storage_only do
    # Stop the storage supervisor
    Supervisor.stop(KVStore.Storage.Supervisor)
    :ok
  end

  @doc """
  Start the full application including HTTP server.
  Use this only when you need to test the HTTP endpoints.
  """
  def start_full_application do
    Application.ensure_all_started(:kv_store)
  end

  @doc """
  Stop the full application.
  """
  def stop_full_application do
    Application.stop(:kv_store)
  end

  @doc """
  Clean up test data directory.
  """
  def cleanup_test_data do
    File.rm_rf("data")
    :ok
  end

  @doc """
  Create a temporary test directory.
  """
  def create_temp_test_dir do
    temp_dir = Path.join(System.tmp_dir!(), "kv_store_test_#{:rand.uniform(1_000_000)}")
    File.mkdir_p!(temp_dir)
    temp_dir
  end

  @doc """
  Start storage components for isolated testing with a temporary directory.
  Returns {test_dir, cleanup_fn} for use in test setup.
  """
  def setup_isolated_storage(opts \\ []) do
    test_dir = create_temp_test_dir()
    segment_max_bytes = Keyword.get(opts, :segment_max_bytes, 100)

    # Store original environment
    original_env = %{
      data_dir: System.get_env("KV_DATA_DIR"),
      segment_max: System.get_env("KV_SEGMENT_MAX_BYTES"),
      cluster_enabled: System.get_env("KV_CLUSTER_ENABLED")
    }

    # Set test environment
    System.put_env("KV_DATA_DIR", test_dir)
    System.put_env("KV_SEGMENT_MAX_BYTES", to_string(segment_max_bytes))
    # Disable clustering for storage tests
    System.put_env("KV_CLUSTER_ENABLED", "false")

    # Stop any existing processes
    stop_storage_processes()

    # Wait for processes to stop
    Process.sleep(50)

    # Start storage supervisor (handle case where it's already started)
    case KVStore.Storage.Supervisor.start_link([]) do
      {:ok, _supervisor_pid} ->
        :ok

      {:error, {:already_started, _pid}} ->
        :ok

      {:error, reason} ->
        # Clean up on failure
        File.rm_rf(test_dir)
        restore_environment(original_env)
        raise "Failed to start storage supervisor: #{inspect(reason)}"
    end

    # Wait for all processes to initialize
    Process.sleep(100)

    # Verify all processes are running
    ensure_storage_processes_running()

    cleanup_fn = fn ->
      # Stop storage processes
      try do
        Supervisor.stop(KVStore.Storage.Supervisor)
      catch
        :exit, _ -> :ok
      end

      # Wait for cleanup
      Process.sleep(50)

      # Restore environment
      restore_environment(original_env)

      # Clean up test directory
      File.rm_rf(test_dir)
    end

    {test_dir, cleanup_fn}
  end

  @doc """
  Stop all storage processes if they're running.
  """
  def stop_storage_processes do
    processes = [
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
            if process_name == KVStore.Storage.Supervisor do
              Supervisor.stop(process_name)
            else
              GenServer.stop(process_name, :normal)
            end
          catch
            :exit, _ -> :ok
          end
      end
    end)
  end

  defp restore_environment(original_env) do
    if original_env.data_dir do
      System.put_env("KV_DATA_DIR", original_env.data_dir)
    else
      System.delete_env("KV_DATA_DIR")
    end

    if original_env.segment_max do
      System.put_env("KV_SEGMENT_MAX_BYTES", original_env.segment_max)
    else
      System.delete_env("KV_SEGMENT_MAX_BYTES")
    end

    if original_env.cluster_enabled do
      System.put_env("KV_CLUSTER_ENABLED", original_env.cluster_enabled)
    else
      System.delete_env("KV_CLUSTER_ENABLED")
    end
  end
end
