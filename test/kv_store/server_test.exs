defmodule KVStore.ServerTest do
  use ExUnit.Case, async: false
  use Plug.Test

  alias KVStore.Server

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

  test "GET /kv/:key returns value when key exists" do
    # First put a value
    KVStore.put("test_key", "test_value")

    # Then get it via HTTP
    conn = conn(:get, "/kv/test_key")
    conn = Server.call(conn, Server.init([]))

    assert conn.status == 200
    assert conn.resp_body =~ "test_value"

    response = Jason.decode!(conn.resp_body)
    assert response["key"] == "test_key"
    assert response["value"] == "test_value"
  end

  test "GET /kv/:key returns 404 when key does not exist" do
    conn = conn(:get, "/kv/nonexistent_key")
    conn = Server.call(conn, Server.init([]))

    assert conn.status == 404
    assert conn.resp_body =~ "Key not found"

    response = Jason.decode!(conn.resp_body)
    assert response["error"] == "Key not found"
    assert response["key"] == "nonexistent_key"
  end

  test "PUT /kv/:key stores a value" do
    conn =
      conn(:put, "/kv/new_key")
      |> put_req_header("content-type", "application/json")
      |> assign(:body_params, %{"value" => "new_value"})

    conn = Server.call(conn, Server.init([]))

    assert conn.status == 200

    response = Jason.decode!(conn.resp_body)
    assert response["key"] == "new_key"
    assert is_integer(response["offset"])

    # Verify the value was actually stored
    assert {:ok, "new_value"} = KVStore.get("new_key")
  end

  test "PUT /kv/:key returns 400 when value is missing" do
    conn =
      conn(:put, "/kv/new_key")
      |> put_req_header("content-type", "application/json")
      |> assign(:body_params, %{})

    conn = Server.call(conn, Server.init([]))

    assert conn.status == 400
    assert conn.resp_body =~ "Missing 'value' field"
  end

  test "DELETE /kv/:key deletes a key" do
    # First put a value
    KVStore.put("delete_key", "delete_value")
    assert {:ok, "delete_value"} = KVStore.get("delete_key")

    # Then delete it via HTTP
    conn = conn(:delete, "/kv/delete_key")
    conn = Server.call(conn, Server.init([]))

    assert conn.status == 200

    response = Jason.decode!(conn.resp_body)
    assert response["key"] == "delete_key"
    assert response["deleted"] == true
    assert is_integer(response["offset"])

    # Verify the key was actually deleted
    assert {:error, :not_found} = KVStore.get("delete_key")
  end

  test "GET /kv/range returns range of values" do
    # Put some values
    KVStore.put("a_key", "a_value")
    KVStore.put("b_key", "b_value")
    KVStore.put("c_key", "c_value")
    KVStore.put("d_key", "d_value")

    # Get range via HTTP
    conn = conn(:get, "/kv/range?start=a_key&end=c_key")
    conn = Server.call(conn, Server.init([]))

    assert conn.status == 200

    response = Jason.decode!(conn.resp_body)
    assert response["start"] == "a_key"
    assert response["end"] == "c_key"
    assert length(response["results"]) >= 3

    # Verify results contain expected keys
    keys = Enum.map(response["results"], fn [key, _value] -> key end)
    assert "a_key" in keys
    assert "b_key" in keys
    assert "c_key" in keys
  end

  test "GET /kv/range returns 400 when parameters are missing" do
    conn = conn(:get, "/kv/range")
    conn = Server.call(conn, Server.init([]))

    assert conn.status == 400
    assert conn.resp_body =~ "Missing 'start' or 'end'"
  end

  test "POST /kv/batch performs batch operations" do
    operations = [
      %{"type" => "put", "key" => "batch_key1", "value" => "batch_value1"},
      %{"type" => "put", "key" => "batch_key2", "value" => "batch_value2"},
      %{"type" => "delete", "key" => "batch_key3"}
    ]

    conn =
      conn(:post, "/kv/batch")
      |> put_req_header("content-type", "application/json")
      |> assign(:body_params, %{"operations" => operations})

    conn = Server.call(conn, Server.init([]))

    assert conn.status == 200

    response = Jason.decode!(conn.resp_body)
    assert response["total_operations"] == 3
    assert response["puts"]["puts"] == 2
    assert length(response["deletes"]) == 1

    # Verify puts worked
    assert {:ok, "batch_value1"} = KVStore.get("batch_key1")
    assert {:ok, "batch_value2"} = KVStore.get("batch_key2")
  end

  test "POST /kv/batch returns 400 when operations are missing" do
    conn =
      conn(:post, "/kv/batch")
      |> put_req_header("content-type", "application/json")
      |> assign(:body_params, %{})

    conn = Server.call(conn, Server.init([]))

    assert conn.status == 400
    assert conn.resp_body =~ "Missing or invalid 'operations'"
  end

  test "GET /status returns system status" do
    conn = conn(:get, "/status")
    conn = Server.call(conn, Server.init([]))

    assert conn.status == 200

    response = Jason.decode!(conn.resp_body)
    assert Map.has_key?(response, "storage")
    assert Map.has_key?(response["storage"], "active_segment_id")
    assert Map.has_key?(response["storage"], "keydir_size")
  end

  test "GET /health returns health status" do
    conn = conn(:get, "/health")
    conn = Server.call(conn, Server.init([]))

    assert conn.status == 200

    response = Jason.decode!(conn.resp_body)
    assert response["status"] == "healthy"
    assert Map.has_key?(response, "timestamp")
  end

  test "unmatched routes return 404" do
    conn = conn(:get, "/nonexistent")
    conn = Server.call(conn, Server.init([]))

    assert conn.status == 404
    assert conn.resp_body =~ "Not found"
  end

  test "concurrent requests work correctly" do
    # Start multiple concurrent requests
    tasks =
      for i <- 1..10 do
        Task.async(fn ->
          key = "concurrent_key_#{i}"
          value = "concurrent_value_#{i}"

          # PUT request
          put_conn =
            conn(:put, "/kv/#{key}")
            |> put_req_header("content-type", "application/json")
            |> assign(:body_params, %{"value" => value})

          put_conn = Server.call(put_conn, Server.init([]))

          # GET request
          get_conn = conn(:get, "/kv/#{key}")
          get_conn = Server.call(get_conn, Server.init([]))

          {put_conn.status, get_conn.status}
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
