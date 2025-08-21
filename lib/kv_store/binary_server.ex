defmodule KVStore.BinaryServer do
  @moduledoc """
  Binary protocol server for KVStore providing high-performance network access.

  Protocol format:
  - 4 bytes: message length (big endian)
  - 1 byte: operation type
  - N bytes: payload (operation-specific)

  Operation types:
  - 1: GET
  - 2: PUT
  - 3: DELETE
  - 4: RANGE
  - 5: BATCH_PUT
  - 6: STATUS
  """

  use GenServer
  require Logger

  @doc """
  Child spec for supervisor.
  """
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5000
    }
  end

  # Use next port for binary protocol
  @port KVStore.Config.server_port() + 1

  def start_link(opts \\ []) do
    port = Keyword.get(opts, :port, @port)
    GenServer.start_link(__MODULE__, port, name: __MODULE__)
  end

  @impl true
  def init(port) do
    Logger.info("Starting KVStore binary server on port #{port}")

    case :gen_tcp.listen(port, [:binary, {:packet, 4}, {:active, false}, {:reuseaddr, true}]) do
      {:ok, socket} ->
        {:ok, %{socket: socket, port: port}, {:continue, :accept}}

      {:error, reason} ->
        Logger.error("Failed to start binary server: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  @impl true
  def handle_continue(:accept, state) do
    case :gen_tcp.accept(state.socket) do
      {:ok, client_socket} ->
        # Spawn a new process to handle this client
        Task.start(fn -> handle_client(client_socket) end)
        {:noreply, state, {:continue, :accept}}

      {:error, reason} ->
        Logger.error("Failed to accept connection: #{inspect(reason)}")
        {:noreply, state, {:continue, :accept}}
    end
  end

  @impl true
  def terminate(_reason, state) do
    :gen_tcp.close(state.socket)
  end

  defp handle_client(socket) do
    case handle_client_loop(socket) do
      :ok ->
        :gen_tcp.close(socket)

      {:error, reason} ->
        Logger.error("Client connection error: #{inspect(reason)}")
        :gen_tcp.close(socket)
    end
  end

  defp handle_client_loop(socket) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, <<op_type::8, payload::binary>>} ->
        case handle_operation(op_type, payload) do
          {:ok, response} ->
            :gen_tcp.send(socket, response)
            handle_client_loop(socket)

          {:error, reason} ->
            error_response = encode_error(reason)
            :gen_tcp.send(socket, error_response)
            handle_client_loop(socket)
        end

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_operation(1, payload) do
    # GET operation
    case decode_string(payload) do
      {:ok, key} ->
        case KVStore.get(key) do
          {:ok, value} ->
            response = encode_get_response(key, value)
            {:ok, response}

          {:error, :not_found} ->
            response = encode_not_found_response(key)
            {:ok, response}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_operation(2, payload) do
    # PUT operation
    case decode_kv_pair(payload) do
      {:ok, {key, value}} ->
        case KVStore.put(key, value) do
          {:ok, offset} ->
            response = encode_put_response(key, offset)
            {:ok, response}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_operation(3, payload) do
    # DELETE operation
    case decode_string(payload) do
      {:ok, key} ->
        case KVStore.delete(key) do
          {:ok, offset} ->
            response = encode_delete_response(key, offset)
            {:ok, response}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_operation(4, payload) do
    # RANGE operation
    case decode_range_params(payload) do
      {:ok, {start_key, end_key}} ->
        case KVStore.range(start_key, end_key) do
          {:ok, results} ->
            response = encode_range_response(start_key, end_key, results)
            {:ok, response}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_operation(5, payload) do
    # BATCH_PUT operation
    case decode_kv_pairs(payload) do
      {:ok, kv_pairs} ->
        case KVStore.batch_put(kv_pairs) do
          {:ok, offset} ->
            response = encode_batch_put_response(offset, length(kv_pairs))
            {:ok, response}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_operation(6, _payload) do
    # STATUS operation
    status = KVStore.status()
    response = encode_status_response(status)
    {:ok, response}
  end

  defp handle_operation(_op_type, _payload) do
    {:error, :unknown_operation}
  end

  # Encoding functions
  defp encode_get_response(key, value) do
    <<1::8, encode_string(key) <> encode_string(value)::binary>>
  end

  defp encode_put_response(key, offset) do
    <<2::8, encode_string(key) <> encode_integer(offset)::binary>>
  end

  defp encode_delete_response(key, offset) do
    <<3::8, encode_string(key) <> encode_integer(offset)::binary>>
  end

  defp encode_range_response(start_key, end_key, results) do
    results_binary = encode_kv_pairs(results)
    <<4::8, encode_string(start_key) <> encode_string(end_key) <> results_binary::binary>>
  end

  defp encode_batch_put_response(offset, count) do
    <<5::8, encode_integer(offset) <> encode_integer(count)::binary>>
  end

  defp encode_status_response(status) do
    status_json = Jason.encode!(status)
    <<6::8, encode_string(status_json)::binary>>
  end

  defp encode_not_found_response(key) do
    <<255::8, encode_string(key)::binary>>
  end

  defp encode_error(reason) do
    reason_str = inspect(reason)
    <<254::8, encode_string(reason_str)::binary>>
  end

  # Helper encoding functions
  defp encode_string(str) do
    str_binary = to_string(str)
    <<byte_size(str_binary)::32, str_binary::binary>>
  end

  defp encode_integer(int) do
    <<int::64>>
  end

  defp encode_kv_pairs(pairs) do
    count = length(pairs)

    pairs_binary =
      Enum.map_join(pairs, "", fn {key, value} ->
        encode_string(key) <> encode_string(value)
      end)

    <<count::32, pairs_binary::binary>>
  end

  # Decoding functions
  defp decode_string(<<len::32, str::binary-size(len), rest::binary>>) do
    {:ok, str, rest}
  end

  defp decode_string(_) do
    {:error, :invalid_string}
  end

  defp decode_kv_pair(payload) do
    case decode_string(payload) do
      {:ok, key, rest} ->
        case decode_string(rest) do
          {:ok, value, _} ->
            {:ok, {key, value}}

          _ ->
            {:error, :invalid_value}
        end

      _ ->
        {:error, :invalid_key}
    end
  end

  defp decode_range_params(payload) do
    case decode_string(payload) do
      {:ok, start_key, rest} ->
        case decode_string(rest) do
          {:ok, end_key, _} ->
            {:ok, {start_key, end_key}}

          _ ->
            {:error, :invalid_end_key}
        end

      _ ->
        {:error, :invalid_start_key}
    end
  end

  defp decode_kv_pairs(<<count::32, rest::binary>>) do
    decode_kv_pairs_recursive(rest, count, [])
  end

  defp decode_kv_pairs_recursive(_payload, 0, acc) do
    {:ok, Enum.reverse(acc)}
  end

  defp decode_kv_pairs_recursive(payload, count, acc) do
    case decode_kv_pair(payload) do
      {:ok, {key, value}, rest} ->
        decode_kv_pairs_recursive(rest, count - 1, [{key, value} | acc])

      _ ->
        {:error, :invalid_kv_pair}
    end
  end

  @doc """
  Get the port the binary server is running on.
  """
  def port do
    @port
  end
end
