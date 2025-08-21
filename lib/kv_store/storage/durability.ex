defmodule KVStore.Storage.Durability do
  @moduledoc """
  Durability manager for coordinating crash recovery, WAL operations, and checkpointing.

  This module provides:
  - Atomic operations using WAL
  - Crash recovery coordination
  - Periodic checkpointing
  - Transaction management
  """

  use GenServer
  require Logger

  # Checkpoint every 1000 operations or 60 seconds
  @default_checkpoint_ops 1000
  @default_checkpoint_interval_ms 60_000

  defstruct [
    :wal,
    :data_dir,
    :checkpoint_ops,
    :checkpoint_interval_ms,
    :last_checkpoint,
    :operations_since_checkpoint,
    :checkpoint_timer,
    :sync_policy
  ]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Client API

  @doc """
  Execute an atomic PUT operation.
  """
  def atomic_put(key, value) do
    GenServer.call(__MODULE__, {:atomic_put, key, value})
  end

  @doc """
  Execute an atomic DELETE operation.
  """
  def atomic_delete(key) do
    GenServer.call(__MODULE__, {:atomic_delete, key})
  end

  @doc """
  Execute an atomic BATCH operation.
  """
  def atomic_batch(operations) do
    GenServer.call(__MODULE__, {:atomic_batch, operations})
  end

  @doc """
  Force a checkpoint.
  """
  def checkpoint do
    GenServer.call(__MODULE__, :checkpoint)
  end

  @doc """
  Get durability status.
  """
  def status do
    GenServer.call(__MODULE__, :status)
  end

  # Server callbacks

  @impl true
  def init(opts) do
    data_dir = Keyword.get(opts, :data_dir, "data")
    checkpoint_ops = Keyword.get(opts, :checkpoint_ops, @default_checkpoint_ops)

    checkpoint_interval_ms =
      Keyword.get(opts, :checkpoint_interval_ms, @default_checkpoint_interval_ms)

    sync_policy = Keyword.get(opts, :sync_policy, :sync_on_write)

    # Ensure data directory exists
    File.mkdir_p!(data_dir)

    # Open WAL
    case KVStore.Storage.WAL.open(data_dir, sync_policy) do
      {:ok, wal} ->
        # Set up checkpoint timer
        timer_ref = Process.send_after(self(), :checkpoint_timer, checkpoint_interval_ms)

        state = %__MODULE__{
          wal: wal,
          data_dir: data_dir,
          checkpoint_ops: checkpoint_ops,
          checkpoint_interval_ms: checkpoint_interval_ms,
          last_checkpoint: :os.system_time(:millisecond),
          operations_since_checkpoint: 0,
          checkpoint_timer: timer_ref,
          sync_policy: sync_policy
        }

        Logger.info("Durability manager started with sync_policy=#{sync_policy}")
        {:ok, state}

      {:error, reason} ->
        Logger.error("Failed to open WAL: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  @impl true
  def terminate(_reason, state) do
    # Close WAL on shutdown
    if state.wal do
      KVStore.Storage.WAL.close(state.wal)
    end

    # Cancel timer
    if state.checkpoint_timer do
      Process.cancel_timer(state.checkpoint_timer)
    end

    :ok
  end

  @impl true
  def handle_call({:atomic_put, key, value}, _from, state) do
    transaction_id = generate_transaction_id()

    case execute_atomic_operation(state, :put, {key, value}, transaction_id) do
      {:ok, new_state} ->
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:atomic_delete, key}, _from, state) do
    transaction_id = generate_transaction_id()

    case execute_atomic_operation(state, :delete, key, transaction_id) do
      {:ok, new_state} ->
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:atomic_batch, operations}, _from, state) do
    transaction_id = generate_transaction_id()

    case execute_atomic_batch(state, operations, transaction_id) do
      {:ok, new_state} ->
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:checkpoint, _from, state) do
    case perform_checkpoint(state) do
      {:ok, new_state} ->
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:status, _from, state) do
    status = %{
      operations_since_checkpoint: state.operations_since_checkpoint,
      last_checkpoint: state.last_checkpoint,
      checkpoint_ops: state.checkpoint_ops,
      checkpoint_interval_ms: state.checkpoint_interval_ms,
      wal_offset: state.wal.offset,
      sync_policy: state.sync_policy
    }

    {:reply, status, state}
  end

  @impl true
  def handle_info(:checkpoint_timer, state) do
    # Perform periodic checkpoint
    case perform_checkpoint(state) do
      {:ok, new_state} ->
        # Schedule next checkpoint
        timer_ref = Process.send_after(self(), :checkpoint_timer, state.checkpoint_interval_ms)
        final_state = %{new_state | checkpoint_timer: timer_ref}
        {:noreply, final_state}

      {:error, reason} ->
        Logger.error("Periodic checkpoint failed: #{inspect(reason)}")
        # Continue with next timer anyway
        timer_ref = Process.send_after(self(), :checkpoint_timer, state.checkpoint_interval_ms)
        new_state = %{state | checkpoint_timer: timer_ref}
        {:noreply, new_state}
    end
  end

  # Private functions

  defp execute_atomic_operation(state, operation_type, operation_data, transaction_id) do
    # Write to WAL first
    case write_operation_to_wal(state.wal, operation_type, operation_data, transaction_id) do
      {:ok, new_wal} ->
        # Execute the operation in the storage engine
        case apply_operation_to_engine(operation_type, operation_data) do
          :ok ->
            new_state = %{
              state
              | wal: new_wal,
                operations_since_checkpoint: state.operations_since_checkpoint + 1
            }

            # Check if we need to checkpoint
            if new_state.operations_since_checkpoint >= new_state.checkpoint_ops do
              perform_checkpoint(new_state)
            else
              {:ok, new_state}
            end

          {:error, reason} ->
            Logger.error("Failed to apply operation to engine: #{inspect(reason)}")
            {:error, reason}
        end

      {:error, reason} ->
        Logger.error("Failed to write to WAL: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp execute_atomic_batch(state, operations, transaction_id) do
    # Write batch start to WAL
    case KVStore.Storage.WAL.write_batch_start(state.wal, transaction_id) do
      {:ok, wal_after_start} ->
        # Write all operations to WAL
        case write_batch_operations_to_wal(wal_after_start, operations, transaction_id) do
          {:ok, wal_after_ops} ->
            # Write batch end to WAL
            case KVStore.Storage.WAL.write_batch_end(
                   wal_after_ops,
                   transaction_id,
                   length(operations)
                 ) do
              {:ok, final_wal} ->
                # Execute all operations in the storage engine
                case apply_batch_to_engine(operations) do
                  :ok ->
                    new_state = %{
                      state
                      | wal: final_wal,
                        operations_since_checkpoint:
                          state.operations_since_checkpoint + length(operations)
                    }

                    # Check if we need to checkpoint
                    if new_state.operations_since_checkpoint >= new_state.checkpoint_ops do
                      perform_checkpoint(new_state)
                    else
                      {:ok, new_state}
                    end

                  {:error, reason} ->
                    Logger.error("Failed to apply batch to engine: #{inspect(reason)}")
                    {:error, reason}
                end

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

  defp write_operation_to_wal(wal, :put, {key, value}, transaction_id) do
    KVStore.Storage.WAL.write_put(wal, key, value, transaction_id)
  end

  defp write_operation_to_wal(wal, :delete, key, transaction_id) do
    KVStore.Storage.WAL.write_delete(wal, key, transaction_id)
  end

  defp write_batch_operations_to_wal(wal, operations, transaction_id) do
    Enum.reduce_while(operations, {:ok, wal}, fn operation, {:ok, current_wal} ->
      case operation do
        {:put, key, value} ->
          case KVStore.Storage.WAL.write_put(current_wal, key, value, transaction_id) do
            {:ok, new_wal} -> {:cont, {:ok, new_wal}}
            {:error, reason} -> {:halt, {:error, reason}}
          end

        {:delete, key} ->
          case KVStore.Storage.WAL.write_delete(current_wal, key, transaction_id) do
            {:ok, new_wal} -> {:cont, {:ok, new_wal}}
            {:error, reason} -> {:halt, {:error, reason}}
          end

        _ ->
          {:halt, {:error, :invalid_operation}}
      end
    end)
  end

  defp apply_operation_to_engine(:put, {key, value}) do
    case KVStore.Storage.Engine.put(key, value) do
      {:ok, _offset} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp apply_operation_to_engine(:delete, key) do
    case KVStore.Storage.Engine.delete(key) do
      {:ok, _offset} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp apply_batch_to_engine(operations) do
    kv_pairs =
      Enum.map(operations, fn
        {:put, key, value} -> {key, value}
        # Special marker for deletes
        {:delete, key} -> {key, :delete}
      end)

    case KVStore.Storage.Engine.batch_put(kv_pairs) do
      {:ok, _offset} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp perform_checkpoint(state) do
    Logger.info("Performing checkpoint")

    checkpoint_data = %{
      timestamp: :os.system_time(:millisecond),
      operations_processed: state.operations_since_checkpoint
    }

    case KVStore.Storage.WAL.write_checkpoint(state.wal, checkpoint_data) do
      {:ok, new_wal} ->
        # After successful checkpoint, we could truncate the WAL
        # For now, we'll keep it for debugging purposes

        new_state = %{
          state
          | wal: new_wal,
            last_checkpoint: checkpoint_data.timestamp,
            operations_since_checkpoint: 0
        }

        Logger.info("Checkpoint completed successfully")
        {:ok, new_state}

      {:error, reason} ->
        Logger.error("Checkpoint failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp generate_transaction_id do
    :erlang.unique_integer([:positive, :monotonic])
  end

  @doc """
  Perform WAL replay for crash recovery.
  """
  def replay_wal(data_dir) do
    wal_path = Path.join(data_dir, "wal.log")

    Logger.info("Starting WAL replay for crash recovery")

    KVStore.Storage.WAL.replay(wal_path, fn entry ->
      replay_wal_entry(entry)
    end)
  end

  defp replay_wal_entry(%{type: type, data: data}) do
    %{
      put: put_type,
      delete: delete_type,
      batch_start: batch_start_type,
      batch_end: batch_end_type,
      checkpoint: checkpoint_type
    } = KVStore.Storage.WAL.entry_types()

    case type do
      ^put_type ->
        case KVStore.Storage.Engine.put(data.key, data.value) do
          {:ok, _offset} -> :ok
          {:error, reason} -> {:error, reason}
        end

      ^delete_type ->
        case KVStore.Storage.Engine.delete(data.key) do
          {:ok, _offset} -> :ok
          {:error, reason} -> {:error, reason}
        end

      ^batch_start_type ->
        # For now, we'll replay individual operations
        # In a more sophisticated implementation, we'd track batch boundaries
        :ok

      ^batch_end_type ->
        :ok

      ^checkpoint_type ->
        Logger.info("Replayed checkpoint: #{inspect(data)}")
        :ok

      _ ->
        Logger.warning("Unknown WAL entry type: #{type}")
        :ok
    end
  end
end
