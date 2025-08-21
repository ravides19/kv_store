defmodule KVStore.ClusterClient do
  @moduledoc """
  Cluster-aware client for KVStore that handles failover and load balancing.

  This client can connect to multiple nodes in a cluster and automatically
  handle failover when nodes become unavailable.
  """

  defstruct [
    :nodes,
    :current_leader,
    :protocol,
    :timeout_ms,
    :retry_attempts
  ]

  @type t :: %__MODULE__{
          nodes: [String.t()],
          current_leader: String.t() | nil,
          protocol: :http | :binary,
          timeout_ms: non_neg_integer(),
          retry_attempts: non_neg_integer()
        }

  @doc """
  Create a new cluster client.
  """
  def new(opts \\ []) do
    nodes = Keyword.get(opts, :nodes, KVStore.Config.cluster_nodes())
    protocol = Keyword.get(opts, :protocol, :http)
    timeout_ms = Keyword.get(opts, :timeout_ms, 5000)
    retry_attempts = Keyword.get(opts, :retry_attempts, 3)

    %__MODULE__{
      nodes: nodes,
      current_leader: nil,
      protocol: protocol,
      timeout_ms: timeout_ms,
      retry_attempts: retry_attempts
    }
  end

  @doc """
  Put a key-value pair with automatic failover.
  """
  def put(client, key, value) do
    command = %{operation: :put, key: key, value: value}
    execute_with_failover(client, :put, [key, value], command)
  end

  @doc """
  Get a value with automatic failover.
  """
  def get(client, key) do
    execute_with_failover(client, :get, [key])
  end

  @doc """
  Delete a key with automatic failover.
  """
  def delete(client, key) do
    command = %{operation: :delete, key: key}
    execute_with_failover(client, :delete, [key], command)
  end

  @doc """
  Get a range of keys with automatic failover.
  """
  def range(client, start_key, end_key) do
    execute_with_failover(client, :range, [start_key, end_key])
  end

  @doc """
  Batch put operations with automatic failover.
  """
  def batch_put(client, kv_pairs) do
    command = %{operation: :batch_put, kv_pairs: kv_pairs}
    execute_with_failover(client, :batch_put, [kv_pairs], command)
  end

  @doc """
  Get cluster status.
  """
  def cluster_status(client) do
    execute_with_failover(client, :cluster_status, [])
  end

  @doc """
  Get the current leader.
  """
  def get_leader(client) do
    execute_with_failover(client, :get_leader, [])
  end

  @doc """
  Close the client connection.
  """
  def close(_client) do
    # For cluster client, we don't maintain persistent connections
    # so this is a no-op
    :ok
  end

  # Private functions

  defp execute_with_failover(client, operation, args, command \\ nil) do
    execute_with_failover(client, operation, args, command, 0)
  end

  defp execute_with_failover(_client, _operation, _args, _command, attempts) when attempts >= 3 do
    {:error, :max_retries_exceeded}
  end

  defp execute_with_failover(client, operation, args, command, attempts) do
    # Try current leader first if we have one
    case try_operation(client, client.current_leader, operation, args, command) do
      {:ok, result} ->
        {:ok, result}

      {:error, _reason} ->
        # Try to find a new leader
        case find_leader(client) do
          {:ok, new_leader} ->
            new_client = %{client | current_leader: new_leader}

            case try_operation(new_client, new_leader, operation, args, command) do
              {:ok, result} ->
                {:ok, result}

              {:error, _reason} ->
                # Try other nodes
                try_all_nodes(client, operation, args, command, attempts + 1)
            end

          {:error, _reason} ->
            try_all_nodes(client, operation, args, command, attempts + 1)
        end
    end
  end

  defp try_all_nodes(client, operation, args, command, _attempts) do
    # Try all nodes in round-robin fashion
    Enum.find_value(client.nodes, {:error, :no_available_nodes}, fn node ->
      case try_operation(client, node, operation, args, command) do
        {:ok, result} ->
          {:ok, result}

        {:error, _reason} ->
          nil
      end
    end)
  end

  defp try_operation(client, node, operation, args, command) do
    case client.protocol do
      :http ->
        try_http_operation(client, node, operation, args, command)

      :binary ->
        try_binary_operation(client, node, operation, args, command)
    end
  end

  defp try_http_operation(client, node, operation, args, _command) do
    base_url = build_http_url(node)

    case operation do
      :put ->
        [key, value] = args
        url = "#{base_url}/kv/#{key}"
        body = Jason.encode!(%{value: value})

        case HTTPoison.put(url, body, [{"Content-Type", "application/json"}],
               timeout: client.timeout_ms
             ) do
          {:ok, %HTTPoison.Response{status_code: 200, body: body}} ->
            {:ok, Jason.decode!(body)}

          {:ok, %HTTPoison.Response{status_code: code}} ->
            {:error, {:http_error, code}}

          {:error, reason} ->
            {:error, reason}
        end

      :get ->
        [key] = args
        url = "#{base_url}/kv/#{key}"

        case HTTPoison.get(url, [], timeout: client.timeout_ms) do
          {:ok, %HTTPoison.Response{status_code: 200, body: body}} ->
            {:ok, Jason.decode!(body)}

          {:ok, %HTTPoison.Response{status_code: 404}} ->
            {:error, :not_found}

          {:ok, %HTTPoison.Response{status_code: code}} ->
            {:error, {:http_error, code}}

          {:error, reason} ->
            {:error, reason}
        end

      :delete ->
        [key] = args
        url = "#{base_url}/kv/#{key}"

        case HTTPoison.delete(url, [], timeout: client.timeout_ms) do
          {:ok, %HTTPoison.Response{status_code: 200, body: body}} ->
            {:ok, Jason.decode!(body)}

          {:ok, %HTTPoison.Response{status_code: code}} ->
            {:error, {:http_error, code}}

          {:error, reason} ->
            {:error, reason}
        end

      :range ->
        [start_key, end_key] = args
        url = "#{base_url}/kv/range?start=#{start_key}&end=#{end_key}"

        case HTTPoison.get(url, [], timeout: client.timeout_ms) do
          {:ok, %HTTPoison.Response{status_code: 200, body: body}} ->
            {:ok, Jason.decode!(body)}

          {:ok, %HTTPoison.Response{status_code: code}} ->
            {:error, {:http_error, code}}

          {:error, reason} ->
            {:error, reason}
        end

      :batch_put ->
        [kv_pairs] = args
        url = "#{base_url}/kv/batch"

        operations =
          Enum.map(kv_pairs, fn {key, value} ->
            %{type: "put", key: key, value: value}
          end)

        body = Jason.encode!(%{operations: operations})

        case HTTPoison.post(url, body, [{"Content-Type", "application/json"}],
               timeout: client.timeout_ms
             ) do
          {:ok, %HTTPoison.Response{status_code: 200, body: body}} ->
            {:ok, Jason.decode!(body)}

          {:ok, %HTTPoison.Response{status_code: code}} ->
            {:error, {:http_error, code}}

          {:error, reason} ->
            {:error, reason}
        end

      :cluster_status ->
        url = "#{base_url}/status"

        case HTTPoison.get(url, [], timeout: client.timeout_ms) do
          {:ok, %HTTPoison.Response{status_code: 200, body: body}} ->
            {:ok, Jason.decode!(body)}

          {:ok, %HTTPoison.Response{status_code: code}} ->
            {:error, {:http_error, code}}

          {:error, reason} ->
            {:error, reason}
        end

      :get_leader ->
        # For HTTP, we need to get cluster status and extract leader
        case try_http_operation(client, node, :cluster_status, [], nil) do
          {:ok, status} ->
            leader = get_in(status, ["cluster", "leader_id"])
            {:ok, leader}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  defp try_binary_operation(_client, _node, _operation, _args, _command) do
    # For binary protocol, we need to implement the binary message format
    # This is a simplified version - in practice, you'd want to reuse
    # the binary protocol implementation from KVStore.BinaryServer

    # For now, return an error indicating binary protocol needs implementation
    {:error, :binary_protocol_not_implemented}
  end

  defp find_leader(client) do
    # Try to find a leader by querying cluster status on all nodes
    Enum.find_value(client.nodes, {:error, :no_leader_found}, fn node ->
      case try_operation(client, node, :get_leader, [], nil) do
        {:ok, leader} when not is_nil(leader) ->
          {:ok, leader}

        _ ->
          nil
      end
    end)
  end

  defp build_http_url(node) do
    # Extract host and port from node identifier
    case String.split(node, ":") do
      [host] ->
        "http://#{host}:8080"

      [host, port] ->
        "http://#{host}:#{port}"

      _ ->
        "http://#{node}:8080"
    end
  end
end
