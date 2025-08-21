defmodule KVStore.BinaryServerTest do
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

  test "GET operation via binary protocol" do
    # First put a value using the local API
    KVStore.put("binary_key", "binary_value")

    # Connect to binary server
    {:ok, socket} =
      :gen_tcp.connect('127.0.0.1', KVStore.BinaryServer.port(), [:binary, {:packet, 4}])

    # Send GET request
    key = "binary_key"
    payload = <<1::8, encode_string(key)::binary>>
    message = <<byte_size(payload)::32, payload::binary>>

    :ok = :gen_tcp.send(socket, message)

    # Receive response
    {:ok, <<len::32, response::binary-size(len)>>} = :gen_tcp.recv(socket, 0)

    # Decode response
    <<1::8, rest::binary>> = response
    {:ok, value, _} = decode_string(rest)

    assert value == "binary_value"

    :gen_tcp.close(socket)
  end

  test "PUT operation via binary protocol" do
    # Connect to binary server
    {:ok, socket} =
      :gen_tcp.connect('127.0.0.1', KVStore.BinaryServer.port(), [:binary, {:packet, 4}])

    # Send PUT request
    key = "new_binary_key"
    value = "new_binary_value"
    payload = <<2::8, encode_string(key) <> encode_string(value)::binary>>
    message = <<byte_size(payload)::32, payload::binary>>

    :ok = :gen_tcp.send(socket, message)

    # Receive response
    {:ok, <<len::32, response::binary-size(len)>>} = :gen_tcp.recv(socket, 0)

    # Decode response
    <<2::8, rest::binary>> = response
    {:ok, _key, rest2} = decode_string(rest)
    {:ok, offset, _} = decode_integer(rest2)

    assert is_integer(offset)

    # Verify the value was stored
    assert {:ok, "new_binary_value"} = KVStore.get("new_binary_key")

    :gen_tcp.close(socket)
  end

  test "DELETE operation via binary protocol" do
    # First put a value
    KVStore.put("delete_binary_key", "delete_binary_value")

    # Connect to binary server
    {:ok, socket} =
      :gen_tcp.connect('127.0.0.1', KVStore.BinaryServer.port(), [:binary, {:packet, 4}])

    # Send DELETE request
    key = "delete_binary_key"
    payload = <<3::8, encode_string(key)::binary>>
    message = <<byte_size(payload)::32, payload::binary>>

    :ok = :gen_tcp.send(socket, message)

    # Receive response
    {:ok, <<len::32, response::binary-size(len)>>} = :gen_tcp.recv(socket, 0)

    # Decode response
    <<3::8, rest::binary>> = response
    {:ok, _key, rest2} = decode_string(rest)
    {:ok, offset, _} = decode_integer(rest2)

    assert is_integer(offset)

    # Verify the key was deleted
    assert {:error, :not_found} = KVStore.get("delete_binary_key")

    :gen_tcp.close(socket)
  end

  test "RANGE operation via binary protocol" do
    # Put some values
    KVStore.put("range_a", "value_a")
    KVStore.put("range_b", "value_b")
    KVStore.put("range_c", "value_c")

    # Connect to binary server
    {:ok, socket} =
      :gen_tcp.connect('127.0.0.1', KVStore.BinaryServer.port(), [:binary, {:packet, 4}])

    # Send RANGE request
    start_key = "range_a"
    end_key = "range_c"
    payload = <<4::8, encode_string(start_key) <> encode_string(end_key)::binary>>
    message = <<byte_size(payload)::32, payload::binary>>

    :ok = :gen_tcp.send(socket, message)

    # Receive response
    {:ok, <<len::32, response::binary-size(len)>>} = :gen_tcp.recv(socket, 0)

    # Decode response
    <<4::8, rest::binary>> = response
    {:ok, _start_key, rest2} = decode_string(rest)
    {:ok, _end_key, rest3} = decode_string(rest2)
    {:ok, results, _} = decode_kv_pairs(rest3)

    assert length(results) >= 3

    # Verify results contain expected keys
    keys = Enum.map(results, fn {key, _value} -> key end)
    assert "range_a" in keys
    assert "range_b" in keys
    assert "range_c" in keys

    :gen_tcp.close(socket)
  end

  test "BATCH_PUT operation via binary protocol" do
    # Connect to binary server
    {:ok, socket} =
      :gen_tcp.connect('127.0.0.1', KVStore.BinaryServer.port(), [:binary, {:packet, 4}])

    # Send BATCH_PUT request
    kv_pairs = [
      {"batch_key1", "batch_value1"},
      {"batch_key2", "batch_value2"}
    ]

    pairs_binary = encode_kv_pairs(kv_pairs)
    payload = <<5::8, pairs_binary::binary>>
    message = <<byte_size(payload)::32, payload::binary>>

    :ok = :gen_tcp.send(socket, message)

    # Receive response
    {:ok, <<len::32, response::binary-size(len)>>} = :gen_tcp.recv(socket, 0)

    # Decode response
    <<5::8, rest::binary>> = response
    {:ok, offset, _} = decode_integer(rest)

    assert is_integer(offset)

    # Verify the values were stored
    assert {:ok, "batch_value1"} = KVStore.get("batch_key1")
    assert {:ok, "batch_value2"} = KVStore.get("batch_key2")

    :gen_tcp.close(socket)
  end

  test "STATUS operation via binary protocol" do
    # Connect to binary server
    {:ok, socket} =
      :gen_tcp.connect('127.0.0.1', KVStore.BinaryServer.port(), [:binary, {:packet, 4}])

    # Send STATUS request
    payload = <<6::8>>
    message = <<byte_size(payload)::32, payload::binary>>

    :ok = :gen_tcp.send(socket, message)

    # Receive response
    {:ok, <<len::32, response::binary-size(len)>>} = :gen_tcp.recv(socket, 0)

    # Decode response
    <<6::8, rest::binary>> = response
    {:ok, status_json, _} = decode_string(rest)

    status = Jason.decode!(status_json)
    assert Map.has_key?(status, "storage")
    assert Map.has_key?(status["storage"], "active_segment_id")

    :gen_tcp.close(socket)
  end

  test "GET operation returns not found for missing key" do
    # Connect to binary server
    {:ok, socket} =
      :gen_tcp.connect('127.0.0.1', KVStore.BinaryServer.port(), [:binary, {:packet, 4}])

    # Send GET request for non-existent key
    key = "nonexistent_binary_key"
    payload = <<1::8, encode_string(key)::binary>>
    message = <<byte_size(payload)::32, payload::binary>>

    :ok = :gen_tcp.send(socket, message)

    # Receive response
    {:ok, <<len::32, response::binary-size(len)>>} = :gen_tcp.recv(socket, 0)

    # Decode response (should be not found)
    <<255::8, rest::binary>> = response
    {:ok, _key, _} = decode_string(rest)

    # This indicates not found
    assert true

    :gen_tcp.close(socket)
  end

  test "concurrent binary connections work correctly" do
    # Start multiple concurrent connections
    tasks =
      for i <- 1..5 do
        Task.async(fn ->
          {:ok, socket} =
            :gen_tcp.connect('127.0.0.1', KVStore.BinaryServer.port(), [:binary, {:packet, 4}])

          key = "concurrent_binary_key_#{i}"
          value = "concurrent_binary_value_#{i}"

          # PUT request
          payload = <<2::8, encode_string(key) <> encode_string(value)::binary>>
          message = <<byte_size(payload)::32, payload::binary>>
          :ok = :gen_tcp.send(socket, message)

          {:ok, <<len::32, response::binary-size(len)>>} = :gen_tcp.recv(socket, 0)

          # GET request
          payload2 = <<1::8, encode_string(key)::binary>>
          message2 = <<byte_size(payload2)::32, payload2::binary>>
          :ok = :gen_tcp.send(socket, message2)

          {:ok, <<len2::32, response2::binary-size(len2)>>} = :gen_tcp.recv(socket, 0)

          :gen_tcp.close(socket)

          {len > 0, len2 > 0}
        end)
      end

    results = Task.await_many(tasks)

    # All requests should succeed
    Enum.each(results, fn {put_success, get_success} ->
      assert put_success
      assert get_success
    end)
  end

  # Helper functions for encoding/decoding

  defp encode_string(str) do
    str_binary = to_string(str)
    <<byte_size(str_binary)::32, str_binary::binary>>
  end

  defp encode_kv_pairs(pairs) do
    count = length(pairs)

    pairs_binary =
      Enum.map_join(pairs, "", fn {key, value} ->
        encode_string(key) <> encode_string(value)
      end)

    <<count::32, pairs_binary::binary>>
  end

  defp decode_string(<<len::32, str::binary-size(len), rest::binary>>) do
    {:ok, str, rest}
  end

  defp decode_string(_) do
    {:error, :invalid_string}
  end

  defp decode_integer(<<int::64, rest::binary>>) do
    {:ok, int, rest}
  end

  defp decode_integer(_) do
    {:error, :invalid_integer}
  end

  defp decode_kv_pairs(<<count::32, rest::binary>>) do
    decode_kv_pairs_recursive(rest, count, [])
  end

  defp decode_kv_pairs_recursive(payload, 0, acc) do
    {:ok, Enum.reverse(acc), payload}
  end

  defp decode_kv_pairs_recursive(payload, count, acc) do
    case decode_string(payload) do
      {:ok, key, rest} ->
        case decode_string(rest) do
          {:ok, value, rest2} ->
            decode_kv_pairs_recursive(rest2, count - 1, [{key, value} | acc])

          _ ->
            {:error, :invalid_value}
        end

      _ ->
        {:error, :invalid_key}
    end
  end
end
