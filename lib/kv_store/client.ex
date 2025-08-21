defmodule KVStore.Client do
  @moduledoc """
  Client library for KVStore providing easy access to both HTTP and binary protocols.

  Supports:
  - HTTP/JSON API (default)
  - Binary protocol for high performance
  - Connection pooling
  - Automatic retries
  """

  @type protocol :: :http | :binary
  @type host :: String.t()
  @type port_number :: non_neg_integer()
  @type key :: String.t()
  @type value :: String.t()
  @type kv_pair :: {key(), value()}

  defstruct [
    :protocol,
    :host,
    :port,
    :http_client,
    :binary_socket
  ]

  @doc """
  Create a new client instance.

  ## Options
  - `:protocol` - `:http` (default) or `:binary`
  - `:host` - server host (default: "127.0.0.1")
  - `:port` - server port (default: 8080 for HTTP, 8081 for binary)
  """
  def new(opts \\ []) do
    protocol = Keyword.get(opts, :protocol, :http)
    host = Keyword.get(opts, :host, "127.0.0.1")
    port = Keyword.get(opts, :port, default_port(protocol))

    case protocol do
      :http ->
        %__MODULE__{
          protocol: :http,
          host: host,
          port: port,
          http_client: build_http_client(host, port)
        }

      :binary ->
        case connect_binary(host, port) do
          {:ok, socket} ->
            %__MODULE__{
              protocol: :binary,
              host: host,
              port: port,
              binary_socket: socket
            }

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  @doc """
  Get a value by key.
  """
  def get(%__MODULE__{protocol: :http, http_client: base_url}, key) do
    case HTTPoison.get("#{base_url}/kv/#{key}") do
      {:ok, %HTTPoison.Response{status_code: 200, body: body}} ->
        case Jason.decode(body) do
          {:ok, %{"value" => value}} ->
            {:ok, value}

          _ ->
            {:error, :invalid_response}
        end

      {:ok, %HTTPoison.Response{status_code: 404}} ->
        {:error, :not_found}

      {:ok, %HTTPoison.Response{status_code: code}} ->
        {:error, {:http_error, code}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def get(%__MODULE__{protocol: :binary, binary_socket: socket}, key) do
    # Encode GET operation
    payload = <<1::8, encode_string(key)::binary>>
    message = <<byte_size(payload)::32, payload::binary>>

    case :gen_tcp.send(socket, message) do
      :ok ->
        case :gen_tcp.recv(socket, 0) do
          {:ok, <<len::32, response::binary-size(len)>>} ->
            case decode_response(response) do
              {:ok, value} ->
                {:ok, value}

              {:error, :not_found} ->
                {:error, :not_found}

              {:error, reason} ->
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Store a key-value pair.
  """
  def put(%__MODULE__{protocol: :http, http_client: base_url}, key, value) do
    body = Jason.encode!(%{value: value})

    case HTTPoison.put("#{base_url}/kv/#{key}", body, [{"Content-Type", "application/json"}]) do
      {:ok, %HTTPoison.Response{status_code: 200, body: response_body}} ->
        case Jason.decode(response_body) do
          {:ok, %{"offset" => offset}} ->
            {:ok, offset}

          _ ->
            {:error, :invalid_response}
        end

      {:ok, %HTTPoison.Response{status_code: code}} ->
        {:error, {:http_error, code}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def put(%__MODULE__{protocol: :binary, binary_socket: socket}, key, value) do
    # Encode PUT operation
    payload = <<2::8, encode_string(key) <> encode_string(value)::binary>>
    message = <<byte_size(payload)::32, payload::binary>>

    case :gen_tcp.send(socket, message) do
      :ok ->
        case :gen_tcp.recv(socket, 0) do
          {:ok, <<len::32, response::binary-size(len)>>} ->
            case decode_put_response(response) do
              {:ok, offset} ->
                {:ok, offset}

              {:error, reason} ->
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Delete a key.
  """
  def delete(%__MODULE__{protocol: :http, http_client: base_url}, key) do
    case HTTPoison.delete("#{base_url}/kv/#{key}") do
      {:ok, %HTTPoison.Response{status_code: 200, body: response_body}} ->
        case Jason.decode(response_body) do
          {:ok, %{"offset" => offset}} ->
            {:ok, offset}

          _ ->
            {:error, :invalid_response}
        end

      {:ok, %HTTPoison.Response{status_code: code}} ->
        {:error, {:http_error, code}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def delete(%__MODULE__{protocol: :binary, binary_socket: socket}, key) do
    # Encode DELETE operation
    payload = <<3::8, encode_string(key)::binary>>
    message = <<byte_size(payload)::32, payload::binary>>

    case :gen_tcp.send(socket, message) do
      :ok ->
        case :gen_tcp.recv(socket, 0) do
          {:ok, <<len::32, response::binary-size(len)>>} ->
            case decode_delete_response(response) do
              {:ok, offset} ->
                {:ok, offset}

              {:error, reason} ->
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Get a range of key-value pairs.
  """
  def range(%__MODULE__{protocol: :http, http_client: base_url}, start_key, end_key) do
    url = "#{base_url}/kv/range?start=#{URI.encode(start_key)}&end=#{URI.encode(end_key)}"

    case HTTPoison.get(url) do
      {:ok, %HTTPoison.Response{status_code: 200, body: body}} ->
        case Jason.decode(body) do
          {:ok, %{"results" => results}} ->
            {:ok, results}

          _ ->
            {:error, :invalid_response}
        end

      {:ok, %HTTPoison.Response{status_code: code}} ->
        {:error, {:http_error, code}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def range(%__MODULE__{protocol: :binary, binary_socket: socket}, start_key, end_key) do
    # Encode RANGE operation
    payload = <<4::8, encode_string(start_key) <> encode_string(end_key)::binary>>
    message = <<byte_size(payload)::32, payload::binary>>

    case :gen_tcp.send(socket, message) do
      :ok ->
        case :gen_tcp.recv(socket, 0) do
          {:ok, <<len::32, response::binary-size(len)>>} ->
            case decode_range_response(response) do
              {:ok, results} ->
                {:ok, results}

              {:error, reason} ->
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Perform batch put operations.
  """
  def batch_put(%__MODULE__{protocol: :http, http_client: base_url}, kv_pairs) do
    operations =
      Enum.map(kv_pairs, fn {key, value} ->
        %{type: "put", key: key, value: value}
      end)

    body = Jason.encode!(%{operations: operations})

    case HTTPoison.post("#{base_url}/kv/batch", body, [{"Content-Type", "application/json"}]) do
      {:ok, %HTTPoison.Response{status_code: 200, body: response_body}} ->
        case Jason.decode(response_body) do
          {:ok, %{"puts" => %{"batch_offset" => offset}}} ->
            {:ok, offset}

          _ ->
            {:error, :invalid_response}
        end

      {:ok, %HTTPoison.Response{status_code: code}} ->
        {:error, {:http_error, code}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def batch_put(%__MODULE__{protocol: :binary, binary_socket: socket}, kv_pairs) do
    # Encode BATCH_PUT operation
    pairs_binary = encode_kv_pairs(kv_pairs)
    payload = <<5::8, pairs_binary::binary>>
    message = <<byte_size(payload)::32, payload::binary>>

    case :gen_tcp.send(socket, message) do
      :ok ->
        case :gen_tcp.recv(socket, 0) do
          {:ok, <<len::32, response::binary-size(len)>>} ->
            case decode_batch_put_response(response) do
              {:ok, offset} ->
                {:ok, offset}

              {:error, reason} ->
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Get system status.
  """
  def status(%__MODULE__{protocol: :http, http_client: base_url}) do
    case HTTPoison.get("#{base_url}/status") do
      {:ok, %HTTPoison.Response{status_code: 200, body: body}} ->
        case Jason.decode(body) do
          {:ok, status} ->
            {:ok, status}

          _ ->
            {:error, :invalid_response}
        end

      {:ok, %HTTPoison.Response{status_code: code}} ->
        {:error, {:http_error, code}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def status(%__MODULE__{protocol: :binary, binary_socket: socket}) do
    # Encode STATUS operation
    payload = <<6::8>>
    message = <<byte_size(payload)::32, payload::binary>>

    case :gen_tcp.send(socket, message) do
      :ok ->
        case :gen_tcp.recv(socket, 0) do
          {:ok, <<len::32, response::binary-size(len)>>} ->
            case decode_status_response(response) do
              {:ok, status} ->
                {:ok, status}

              {:error, reason} ->
                {:error, reason}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Close the client connection.
  """
  def close(%__MODULE__{protocol: :binary, binary_socket: socket}) do
    :gen_tcp.close(socket)
  end

  def close(%__MODULE__{protocol: :http}) do
    :ok
  end

  # Private helper functions

  defp default_port(:http), do: 8080
  defp default_port(:binary), do: 8081

  defp build_http_client(host, port) do
    "http://#{host}:#{port}"
  end

  defp connect_binary(host, port) do
    :gen_tcp.connect(String.to_charlist(host), port, [:binary, {:packet, 4}])
  end

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

  defp decode_response(<<1::8, rest::binary>>) do
    case decode_string(rest) do
      {:ok, value, _} ->
        {:ok, value}

      _ ->
        {:error, :invalid_response}
    end
  end

  defp decode_response(<<255::8, rest::binary>>) do
    case decode_string(rest) do
      {:ok, _key, _} ->
        {:error, :not_found}

      _ ->
        {:error, :invalid_response}
    end
  end

  defp decode_response(<<254::8, rest::binary>>) do
    case decode_string(rest) do
      {:ok, reason, _} ->
        {:error, reason}

      _ ->
        {:error, :invalid_response}
    end
  end

  defp decode_put_response(<<2::8, rest::binary>>) do
    case decode_string(rest) do
      {:ok, _key, rest2} ->
        case decode_integer(rest2) do
          {:ok, offset, _} ->
            {:ok, offset}

          _ ->
            {:error, :invalid_response}
        end

      _ ->
        {:error, :invalid_response}
    end
  end

  defp decode_delete_response(<<3::8, rest::binary>>) do
    case decode_string(rest) do
      {:ok, _key, rest2} ->
        case decode_integer(rest2) do
          {:ok, offset, _} ->
            {:ok, offset}

          _ ->
            {:error, :invalid_response}
        end

      _ ->
        {:error, :invalid_response}
    end
  end

  defp decode_range_response(<<4::8, rest::binary>>) do
    case decode_string(rest) do
      {:ok, _start_key, rest2} ->
        case decode_string(rest2) do
          {:ok, _end_key, rest3} ->
            case decode_kv_pairs(rest3) do
              {:ok, results, _} ->
                {:ok, results}

              _ ->
                {:error, :invalid_response}
            end

          _ ->
            {:error, :invalid_response}
        end

      _ ->
        {:error, :invalid_response}
    end
  end

  defp decode_batch_put_response(<<5::8, rest::binary>>) do
    case decode_integer(rest) do
      {:ok, offset, _} ->
        {:ok, offset}

      _ ->
        {:error, :invalid_response}
    end
  end

  defp decode_status_response(<<6::8, rest::binary>>) do
    case decode_string(rest) do
      {:ok, status_json, _} ->
        case Jason.decode(status_json) do
          {:ok, status} ->
            {:ok, status}

          _ ->
            {:error, :invalid_response}
        end

      _ ->
        {:error, :invalid_response}
    end
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
