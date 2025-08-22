defmodule KVStore.TestServer do
  @moduledoc """
  Test-specific HTTP server that uses the direct storage engine.
  This avoids clustering dependencies in tests.
  """

  use Plug.Router

  plug(:match)
  plug(Plug.Parsers, parsers: [:json], pass: ["text/*"], json_decoder: Jason)
  plug(:dispatch)

  # GET /kv/range?start=:start&end=:end
  get "/kv/range" do
    start_key = conn.query_params["start"]
    end_key = conn.query_params["end"]

    cond do
      is_nil(start_key) or is_nil(end_key) ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Missing 'start' or 'end' query parameters"}))

      true ->
        case KVStore.Storage.Engine.range(start_key, end_key) do
          {:ok, results} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(
              200,
              Jason.encode!(%{
                start_key: start_key,
                end_key: end_key,
                count: length(results),
                pairs: Enum.map(results, fn {k, v} -> %{key: k, value: v} end)
              })
            )

          {:error, reason} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(
              500,
              Jason.encode!(%{error: "Range query failed", reason: inspect(reason)})
            )
        end
    end
  end

  # GET /kv/:key
  get "/kv/:key" do
    case KVStore.Storage.Engine.get(key) do
      {:ok, value} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{key: key, value: value}))

      {:error, :not_found} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(404, Jason.encode!(%{error: "Key not found", key: key}))

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(500, Jason.encode!(%{error: "Internal error", reason: inspect(reason)}))
    end
  end

  # PUT /kv/:key
  put "/kv/:key" do
    case conn.body_params do
      %{"value" => value} ->
        case KVStore.Storage.Engine.put(key, value) do
          {:ok, offset} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(
              200,
              Jason.encode!(%{
                key: key,
                value: value,
                offset: offset,
                message: "Value stored successfully"
              })
            )

          {:error, reason} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(
              500,
              Jason.encode!(%{error: "Failed to store key", reason: inspect(reason)})
            )
        end

      _ ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Missing 'value' field in request body"}))
    end
  end

  # DELETE /kv/:key
  delete "/kv/:key" do
    case KVStore.Storage.Engine.delete(key) do
      {:ok, offset} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          200,
          Jason.encode!(%{
            key: key,
            offset: offset,
            deleted: true,
            message: "Key deleted successfully"
          })
        )

      {:error, reason} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          500,
          Jason.encode!(%{error: "Failed to delete key", reason: inspect(reason)})
        )
    end
  end

  # GET /kv/range?start=:start&end=:end
  get "/kv/range" do
    start_key = conn.query_params["start"]
    end_key = conn.query_params["end"]

    cond do
      is_nil(start_key) or is_nil(end_key) ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Missing 'start' or 'end' query parameters"}))

      true ->
        case KVStore.Storage.Engine.range(start_key, end_key) do
          {:ok, results} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(
              200,
              Jason.encode!(%{
                start_key: start_key,
                end_key: end_key,
                count: length(results),
                pairs: Enum.map(results, fn {k, v} -> %{key: k, value: v} end)
              })
            )

          {:error, reason} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(
              500,
              Jason.encode!(%{error: "Range query failed", reason: inspect(reason)})
            )
        end
    end
  end

  # POST /kv/batch
  post "/kv/batch" do
    case conn.body_params do
      %{"operations" => operations} when is_list(operations) ->
        results =
          Enum.map(operations, fn operation ->
            case operation do
              %{"type" => "put", "key" => key, "value" => value} ->
                case KVStore.Storage.Engine.put(key, value) do
                  {:ok, offset} ->
                    %{operation: "put", key: key, status: "success", offset: offset}

                  {:error, reason} ->
                    %{operation: "put", key: key, status: "error", error: inspect(reason)}
                end

              %{"type" => "delete", "key" => key} ->
                case KVStore.Storage.Engine.delete(key) do
                  {:ok, offset} ->
                    %{operation: "delete", key: key, status: "success", offset: offset}

                  {:error, reason} ->
                    %{operation: "delete", key: key, status: "error", error: inspect(reason)}
                end

              _ ->
                %{operation: "unknown", status: "error", error: "Invalid operation"}
            end
          end)

        successful_ops = Enum.count(results, fn %{status: status} -> status == "success" end)
        failed_ops = length(results) - successful_ops

        response = %{
          operations_count: length(results),
          successful_operations: successful_ops,
          failed_operations: failed_ops,
          results: results
        }

        status_code = if failed_ops > 0, do: 207, else: 200
        status = if failed_ops > 0, do: "partial_success", else: "success"

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(status_code, Jason.encode!(Map.put(response, :status, status)))

      _ ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(400, Jason.encode!(%{error: "Missing or invalid 'operations' field"}))
    end
  end

  # GET /status
  get "/status" do
    try do
      storage_status = KVStore.Storage.Engine.status()
      cache_status = KVStore.Storage.Cache.stats()

      status = %{
        system: %{
          # Simplified for tests
          uptime: "0s",
          version: "0.1.0",
          node_id: "test_node"
        },
        storage: %{
          total_segments: Map.get(storage_status, :total_segments, 1),
          active_segment_id: storage_status.active_segment_id,
          keydir_size: storage_status.keydir_size,
          total_size_bytes: storage_status.total_size_bytes || 0,
          compression_ratio: 0.65
        },
        cache: %{
          entries: cache_status.current_entries,
          max_entries: cache_status.max_entries,
          hit_rate: cache_status.hit_rate || 0.0,
          memory_usage_bytes: cache_status.memory_usage_bytes || 0
        },
        cluster: %{
          enabled: false,
          node_count: 1,
          leader_id: "test_node",
          current_term: 1,
          commit_index: 0
        },
        performance: %{
          operations_per_second: 0,
          average_latency_ms: 0,
          compaction_running: false
        }
      }

      conn
      |> put_resp_content_type("application/json")
      |> send_resp(200, Jason.encode!(status))
    catch
      :exit, _ ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(503, Jason.encode!(%{error: "Service unavailable"}))
    end
  end

  # GET /health
  get "/health" do
    try do
      # Simple health check
      _status = KVStore.Storage.Engine.status()

      conn
      |> put_resp_content_type("application/json")
      |> send_resp(
        200,
        Jason.encode!(%{
          status: "healthy",
          message: "KVStore is running normally",
          timestamp: DateTime.utc_now() |> DateTime.to_iso8601()
        })
      )
    catch
      :exit, _ ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(503, Jason.encode!(%{error: "Service is unhealthy"}))
    end
  end

  # Catch-all for unmatched routes
  match _ do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(404, Jason.encode!(%{error: "Not found"}))
  end
end
