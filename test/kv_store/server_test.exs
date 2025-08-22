defmodule KVStore.ServerTest do
  use ExUnit.Case, async: false

  alias KVStore.ServerTestHelper

  setup do
    # Use isolated server test helper
    {_server_pid, server_port, cleanup_fn} = ServerTestHelper.start_isolated_server()

    on_exit(cleanup_fn)

    {:ok, %{server_port: server_port}}
  end

  test "GET /kv/:key returns value when key exists", %{server_port: port} do
    # First put a value using direct storage engine
    KVStore.Storage.Engine.put("test_key", "test_value")

    # Then get it via HTTP
    {:ok, response} = HTTPoison.get("http://localhost:#{port}/kv/test_key")

    assert response.status_code == 200
    assert response.body =~ "test_value"

    response_data = Jason.decode!(response.body)
    assert response_data["key"] == "test_key"
    assert response_data["value"] == "test_value"
  end

  test "GET /kv/:key returns 404 when key does not exist", %{server_port: port} do
    {:ok, response} = HTTPoison.get("http://localhost:#{port}/kv/nonexistent_key")

    assert response.status_code == 404
    assert response.body =~ "Key not found"

    response_data = Jason.decode!(response.body)
    assert response_data["error"] == "Key not found"
    assert response_data["key"] == "nonexistent_key"
  end

  test "PUT /kv/:key stores a value", %{server_port: port} do
    {:ok, response} =
      HTTPoison.put(
        "http://localhost:#{port}/kv/new_key",
        Jason.encode!(%{"value" => "new_value"}),
        [{"Content-Type", "application/json"}]
      )

    assert response.status_code == 200

    response_data = Jason.decode!(response.body)
    assert response_data["key"] == "new_key"
    assert is_integer(response_data["offset"])

    # Verify the value was actually stored
    assert {:ok, "new_value"} = KVStore.Storage.Engine.get("new_key")
  end

  test "PUT /kv/:key returns 400 when value is missing", %{server_port: port} do
    {:ok, response} =
      HTTPoison.put(
        "http://localhost:#{port}/kv/new_key",
        Jason.encode!(%{}),
        [{"Content-Type", "application/json"}]
      )

    assert response.status_code == 400
    assert response.body =~ "Missing 'value' field"
  end

  test "DELETE /kv/:key deletes a key", %{server_port: port} do
    # First put a value using direct storage engine
    KVStore.Storage.Engine.put("delete_key", "delete_value")
    assert {:ok, "delete_value"} = KVStore.Storage.Engine.get("delete_key")

    # Then delete it via HTTP
    {:ok, response} = HTTPoison.delete("http://localhost:#{port}/kv/delete_key")

    assert response.status_code == 200

    response_data = Jason.decode!(response.body)
    assert response_data["key"] == "delete_key"
    assert response_data["deleted"] == true
    assert is_integer(response_data["offset"])

    # Verify the key was actually deleted
    assert {:error, :not_found} = KVStore.Storage.Engine.get("delete_key")
  end

  test "GET /kv/range returns range of values", %{server_port: port} do
    # Put some values using direct storage engine
    KVStore.Storage.Engine.put("a_key", "a_value")
    KVStore.Storage.Engine.put("b_key", "b_value")
    KVStore.Storage.Engine.put("c_key", "c_value")
    KVStore.Storage.Engine.put("d_key", "d_value")

    # Get range via HTTP
    {:ok, response} = HTTPoison.get("http://localhost:#{port}/kv/range?start=a_key&end=c_key")

    assert response.status_code == 200

    response_data = Jason.decode!(response.body)
    assert response_data["start_key"] == "a_key"
    assert response_data["end_key"] == "c_key"
    assert response_data["count"] >= 3

    # Verify results contain expected keys
    keys = Enum.map(response_data["pairs"], fn %{"key" => key} -> key end)
    assert "a_key" in keys
    assert "b_key" in keys
    assert "c_key" in keys
  end

  test "GET /kv/range returns 400 when parameters are missing", %{server_port: port} do
    {:ok, response} = HTTPoison.get("http://localhost:#{port}/kv/range")

    assert response.status_code == 400
    assert response.body =~ "Missing 'start' or 'end'"
  end

  test "POST /kv/batch performs batch operations", %{server_port: port} do
    operations = [
      %{"type" => "put", "key" => "batch_key1", "value" => "batch_value1"},
      %{"type" => "put", "key" => "batch_key2", "value" => "batch_value2"},
      %{"type" => "delete", "key" => "batch_key3"}
    ]

    {:ok, response} =
      HTTPoison.post(
        "http://localhost:#{port}/kv/batch",
        Jason.encode!(%{"operations" => operations}),
        [{"Content-Type", "application/json"}]
      )

    assert response.status_code == 200

    response_data = Jason.decode!(response.body)
    assert response_data["operations_count"] == 3
    assert response_data["successful_operations"] == 3
    assert response_data["failed_operations"] == 0

    # Verify puts worked
    assert {:ok, "batch_value1"} = KVStore.Storage.Engine.get("batch_key1")
    assert {:ok, "batch_value2"} = KVStore.Storage.Engine.get("batch_key2")
  end

  test "POST /kv/batch returns 400 when operations are missing", %{server_port: port} do
    {:ok, response} =
      HTTPoison.post(
        "http://localhost:#{port}/kv/batch",
        Jason.encode!(%{}),
        [{"Content-Type", "application/json"}]
      )

    assert response.status_code == 400
    assert response.body =~ "Missing or invalid 'operations'"
  end

  test "GET /status returns system status", %{server_port: port} do
    {:ok, response} = HTTPoison.get("http://localhost:#{port}/status")

    assert response.status_code == 200

    response_data = Jason.decode!(response.body)
    assert Map.has_key?(response_data, "storage")
    assert Map.has_key?(response_data["storage"], "active_segment_id")
    assert Map.has_key?(response_data["storage"], "keydir_size")
  end

  test "GET /health returns health status", %{server_port: port} do
    {:ok, response} = HTTPoison.get("http://localhost:#{port}/health")

    assert response.status_code == 200

    response_data = Jason.decode!(response.body)
    assert response_data["status"] == "healthy"
    assert Map.has_key?(response_data, "timestamp")
  end

  test "unmatched routes return 404", %{server_port: port} do
    {:ok, response} = HTTPoison.get("http://localhost:#{port}/nonexistent")

    assert response.status_code == 404
    assert response.body =~ "Not found"
  end

  test "concurrent requests work correctly", %{server_port: port} do
    # Start multiple concurrent requests
    tasks =
      for i <- 1..10 do
        Task.async(fn ->
          key = "concurrent_key_#{i}"
          value = "concurrent_value_#{i}"

          # PUT request
          put_response =
            HTTPoison.put(
              "http://localhost:#{port}/kv/#{key}",
              Jason.encode!(%{"value" => value}),
              [{"Content-Type", "application/json"}]
            )

          # GET request
          get_response = HTTPoison.get("http://localhost:#{port}/kv/#{key}")

          case {put_response, get_response} do
            {{:ok, put_resp}, {:ok, get_resp}} ->
              {put_resp.status_code, get_resp.status_code}

            _ ->
              # Error case
              {500, 500}
          end
        end)
      end

    results = Task.await_many(tasks)

    # All requests should succeed
    Enum.each(results, fn {put_status, get_status} ->
      assert put_status == 200
      assert get_status == 200
    end)
  end
end
