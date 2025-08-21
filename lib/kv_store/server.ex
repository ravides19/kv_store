defmodule KVStore.Server do
  @moduledoc """
  HTTP server for KVStore providing RESTful API endpoints.

  Supports:
  - GET /kv/:key - Retrieve a value
  - PUT /kv/:key - Store a value
  - DELETE /kv/:key - Delete a key
  - GET /kv/range?start=:start&end=:end - Range query
  - POST /kv/batch - Batch operations
  - GET /status - System status
  """

  use Plug.Router
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

  plug(:match)
  plug(Plug.Parsers, parsers: [:json], pass: ["text/*"], json_decoder: Jason)
  plug(:dispatch)

  # GET /kv/:key
  get "/kv/:key" do
    case KVStore.get(key) do
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
        case KVStore.put(key, value) do
          {:ok, offset} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(200, Jason.encode!(%{key: key, offset: offset}))

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
    case KVStore.delete(key) do
      {:ok, offset} ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(%{key: key, offset: offset, deleted: true}))

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
        case KVStore.range(start_key, end_key) do
          {:ok, results} ->
            conn
            |> put_resp_content_type("application/json")
            |> send_resp(200, Jason.encode!(%{start: start_key, end: end_key, results: results}))

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
        # Convert operations to the format expected by batch_put
        kv_pairs =
          Enum.map(operations, fn op ->
            case op do
              %{"type" => "put", "key" => key, "value" => value} ->
                {key, value}

              %{"type" => "delete", "key" => key} ->
                # We'll handle deletes separately
                {key, nil}

              _ ->
                nil
            end
          end)
          |> Enum.reject(&is_nil/1)

        # Handle puts
        put_results =
          case KVStore.batch_put(kv_pairs) do
            {:ok, offset} ->
              %{batch_offset: offset, puts: length(kv_pairs)}

            {:error, reason} ->
              %{error: inspect(reason)}
          end

        # Handle deletes separately (since batch_put doesn't handle deletes)
        delete_operations =
          Enum.filter(operations, fn op ->
            op["type"] == "delete"
          end)

        delete_results =
          Enum.map(delete_operations, fn %{"key" => key} ->
            case KVStore.delete(key) do
              {:ok, offset} -> %{key: key, deleted: true, offset: offset}
              {:error, reason} -> %{key: key, deleted: false, error: inspect(reason)}
            end
          end)

        response = %{
          puts: put_results,
          deletes: delete_results,
          total_operations: length(operations)
        }

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, Jason.encode!(response))

      _ ->
        conn
        |> put_resp_content_type("application/json")
        |> send_resp(
          400,
          Jason.encode!(%{error: "Missing or invalid 'operations' array in request body"})
        )
    end
  end

  # GET /status
  get "/status" do
    status = KVStore.status()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(status))
  end

  # GET /health
  get "/health" do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, Jason.encode!(%{status: "healthy", timestamp: DateTime.utc_now()}))
  end

  # Catch-all for unmatched routes
  match _ do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(404, Jason.encode!(%{error: "Not found"}))
  end

  @doc """
  Start the HTTP server.
  """
  def start_link(opts \\ []) do
    port = Keyword.get(opts, :port, KVStore.Config.server_port())
    host = Keyword.get(opts, :host, KVStore.Config.server_host())

    Logger.info("Starting KVStore HTTP server on #{host}:#{port}")

    Plug.Cowboy.http(__MODULE__, [], port: port)
  end

  @doc """
  Stop the HTTP server.
  """
  def stop do
    Logger.info("Stopping KVStore HTTP server")
    Plug.Cowboy.shutdown(__MODULE__.HTTP)
  end
end
