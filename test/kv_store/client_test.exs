defmodule KVStore.ClientTest do
  use ExUnit.Case, async: false

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

    # Wait for servers to start
    Process.sleep(200)

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

  describe "HTTP client" do
    test "creates HTTP client successfully" do
      client = KVStore.Client.new(protocol: :http)
      assert client.protocol == :http
      assert client.host == "127.0.0.1"
      assert client.port == 8080
    end

    test "PUT and GET operations work" do
      client = KVStore.Client.new(protocol: :http)

      # PUT operation
      {:ok, offset} = KVStore.Client.put(client, "http_key", "http_value")
      assert is_integer(offset)

      # GET operation
      {:ok, value} = KVStore.Client.get(client, "http_key")
      assert value == "http_value"
    end

    test "DELETE operation works" do
      client = KVStore.Client.new(protocol: :http)

      # PUT first
      {:ok, _} = KVStore.Client.put(client, "delete_http_key", "delete_http_value")

      # Verify it exists
      {:ok, "delete_http_value"} = KVStore.Client.get(client, "delete_http_key")

      # DELETE
      {:ok, offset} = KVStore.Client.delete(client, "delete_http_key")
      assert is_integer(offset)

      # Verify it's gone
      {:error, :not_found} = KVStore.Client.get(client, "delete_http_key")
    end

    test "RANGE operation works" do
      client = KVStore.Client.new(protocol: :http)

      # Put some values
      {:ok, _} = KVStore.Client.put(client, "range_a", "value_a")
      {:ok, _} = KVStore.Client.put(client, "range_b", "value_b")
      {:ok, _} = KVStore.Client.put(client, "range_c", "value_c")

      # Get range
      {:ok, results} = KVStore.Client.range(client, "range_a", "range_c")
      assert length(results) >= 3

      # Verify results contain expected keys
      keys = Enum.map(results, fn [key, _value] -> key end)
      assert "range_a" in keys
      assert "range_b" in keys
      assert "range_c" in keys
    end

    test "BATCH_PUT operation works" do
      client = KVStore.Client.new(protocol: :http)

      kv_pairs = [
        {"batch_http_key1", "batch_http_value1"},
        {"batch_http_key2", "batch_http_value2"}
      ]

      {:ok, offset} = KVStore.Client.batch_put(client, kv_pairs)
      assert is_integer(offset)

      # Verify values were stored
      {:ok, "batch_http_value1"} = KVStore.Client.get(client, "batch_http_key1")
      {:ok, "batch_http_value2"} = KVStore.Client.get(client, "batch_http_key2")
    end

    test "STATUS operation works" do
      client = KVStore.Client.new(protocol: :http)

      {:ok, status} = KVStore.Client.status(client)
      assert Map.has_key?(status, "storage")
      assert Map.has_key?(status["storage"], "active_segment_id")
    end

    test "GET returns not found for missing key" do
      client = KVStore.Client.new(protocol: :http)

      {:error, :not_found} = KVStore.Client.get(client, "nonexistent_http_key")
    end

    test "close operation works" do
      client = KVStore.Client.new(protocol: :http)

      # Should not raise any error
      :ok = KVStore.Client.close(client)
    end
  end

  describe "Binary client" do
    test "creates binary client successfully" do
      client = KVStore.Client.new(protocol: :binary)
      assert client.protocol == :binary
      assert client.host == "127.0.0.1"
      assert client.port == 8081
    end

    test "PUT and GET operations work" do
      client = KVStore.Client.new(protocol: :binary)

      # PUT operation
      {:ok, offset} = KVStore.Client.put(client, "binary_key", "binary_value")
      assert is_integer(offset)

      # GET operation
      {:ok, value} = KVStore.Client.get(client, "binary_key")
      assert value == "binary_value"
    end

    test "DELETE operation works" do
      client = KVStore.Client.new(protocol: :binary)

      # PUT first
      {:ok, _} = KVStore.Client.put(client, "delete_binary_key", "delete_binary_value")

      # Verify it exists
      {:ok, "delete_binary_value"} = KVStore.Client.get(client, "delete_binary_key")

      # DELETE
      {:ok, offset} = KVStore.Client.delete(client, "delete_binary_key")
      assert is_integer(offset)

      # Verify it's gone
      {:error, :not_found} = KVStore.Client.get(client, "delete_binary_key")
    end

    test "RANGE operation works" do
      client = KVStore.Client.new(protocol: :binary)

      # Put some values
      {:ok, _} = KVStore.Client.put(client, "range_a", "value_a")
      {:ok, _} = KVStore.Client.put(client, "range_b", "value_b")
      {:ok, _} = KVStore.Client.put(client, "range_c", "value_c")

      # Get range
      {:ok, results} = KVStore.Client.range(client, "range_a", "range_c")
      assert length(results) >= 3

      # Verify results contain expected keys
      keys = Enum.map(results, fn {key, _value} -> key end)
      assert "range_a" in keys
      assert "range_b" in keys
      assert "range_c" in keys
    end

    test "BATCH_PUT operation works" do
      client = KVStore.Client.new(protocol: :binary)

      kv_pairs = [
        {"batch_binary_key1", "batch_binary_value1"},
        {"batch_binary_key2", "batch_binary_value2"}
      ]

      {:ok, offset} = KVStore.Client.batch_put(client, kv_pairs)
      assert is_integer(offset)

      # Verify values were stored
      {:ok, "batch_binary_value1"} = KVStore.Client.get(client, "batch_binary_key1")
      {:ok, "batch_binary_value2"} = KVStore.Client.get(client, "batch_binary_key2")
    end

    test "STATUS operation works" do
      client = KVStore.Client.new(protocol: :binary)

      {:ok, status} = KVStore.Client.status(client)
      assert Map.has_key?(status, "storage")
      assert Map.has_key?(status["storage"], "active_segment_id")
    end

    test "GET returns not found for missing key" do
      client = KVStore.Client.new(protocol: :binary)

      {:error, :not_found} = KVStore.Client.get(client, "nonexistent_binary_key")
    end

    test "close operation works" do
      client = KVStore.Client.new(protocol: :binary)

      # Should not raise any error
      :ok = KVStore.Client.close(client)
    end
  end

  describe "Client configuration" do
    test "uses default configuration" do
      client = KVStore.Client.new()
      assert client.protocol == :http
      assert client.host == "127.0.0.1"
      assert client.port == 8080
    end

    test "accepts custom host and port" do
      client = KVStore.Client.new(host: "localhost", port: 9090)
      assert client.host == "localhost"
      assert client.port == 9090
    end

    test "uses correct default ports for protocols" do
      http_client = KVStore.Client.new(protocol: :http)
      assert http_client.port == 8080

      binary_client = KVStore.Client.new(protocol: :binary)
      assert binary_client.port == 8081
    end
  end

  describe "Error handling" do
    test "handles connection errors gracefully" do
      # Try to connect to non-existent server
      {:error, _reason} = KVStore.Client.new(protocol: :binary, port: 9999)
    end

    test "handles server errors gracefully" do
      client = KVStore.Client.new(protocol: :http)

      # This should not crash even if server is not responding properly
      # (though in our test setup it should work)
      try do
        KVStore.Client.get(client, "test_key")
      catch
        :exit, _ -> :ok
        :error, _ -> :ok
      end
    end
  end

  describe "Concurrent operations" do
    test "multiple clients can operate concurrently" do
      # Create multiple clients
      clients =
        for i <- 1..3 do
          KVStore.Client.new(protocol: :http)
        end

      # Perform concurrent operations
      tasks =
        for {client, i} <- Enum.with_index(clients) do
          Task.async(fn ->
            key = "concurrent_client_key_#{i}"
            value = "concurrent_client_value_#{i}"

            {:ok, _} = KVStore.Client.put(client, key, value)
            {:ok, retrieved_value} = KVStore.Client.get(client, key)

            retrieved_value
          end)
        end

      results = Task.await_many(tasks)

      # All operations should succeed
      assert length(results) == 3
      assert "concurrent_client_value_0" in results
      assert "concurrent_client_value_1" in results
      assert "concurrent_client_value_2" in results
    end
  end
end
