defmodule KVStore.Storage.WALTest do
  use ExUnit.Case

  setup do
    # Create a temporary directory for testing
    test_dir = Path.join(System.tmp_dir!(), "kv_store_wal_test_#{:rand.uniform(1_000_000)}")
    File.rm_rf!(test_dir)
    File.mkdir_p!(test_dir)

    on_exit(fn ->
      File.rm_rf!(test_dir)
    end)

    {:ok, test_dir: test_dir}
  end

  test "open and close WAL file", %{test_dir: test_dir} do
    {:ok, wal} = KVStore.Storage.WAL.open(test_dir, :sync_on_write)

    assert wal.path == Path.join(test_dir, "wal.log")
    assert wal.offset == 0
    assert wal.sync_policy == :sync_on_write

    assert :ok = KVStore.Storage.WAL.close(wal)
  end

  test "write PUT operation to WAL", %{test_dir: test_dir} do
    {:ok, wal} = KVStore.Storage.WAL.open(test_dir, :no_sync)

    {:ok, updated_wal} = KVStore.Storage.WAL.write_put(wal, "test_key", "test_value", 12345)

    assert updated_wal.offset > wal.offset
    assert File.exists?(wal.path)

    KVStore.Storage.WAL.close(updated_wal)
  end

  test "write DELETE operation to WAL", %{test_dir: test_dir} do
    {:ok, wal} = KVStore.Storage.WAL.open(test_dir, :no_sync)

    {:ok, updated_wal} = KVStore.Storage.WAL.write_delete(wal, "test_key", 12345)

    assert updated_wal.offset > wal.offset

    KVStore.Storage.WAL.close(updated_wal)
  end

  test "write batch operations to WAL", %{test_dir: test_dir} do
    {:ok, wal} = KVStore.Storage.WAL.open(test_dir, :no_sync)

    transaction_id = 12345

    # Write batch start
    {:ok, wal2} = KVStore.Storage.WAL.write_batch_start(wal, transaction_id)
    assert wal2.offset > wal.offset

    # Write batch end
    {:ok, wal3} = KVStore.Storage.WAL.write_batch_end(wal2, transaction_id, 5)
    assert wal3.offset > wal2.offset

    KVStore.Storage.WAL.close(wal3)
  end

  test "write checkpoint to WAL", %{test_dir: test_dir} do
    {:ok, wal} = KVStore.Storage.WAL.open(test_dir, :no_sync)

    checkpoint_data = %{
      timestamp: :os.system_time(:millisecond),
      operations_processed: 100
    }

    {:ok, updated_wal} = KVStore.Storage.WAL.write_checkpoint(wal, checkpoint_data)

    assert updated_wal.offset > wal.offset

    KVStore.Storage.WAL.close(updated_wal)
  end

  test "replay empty WAL", %{test_dir: test_dir} do
    wal_path = Path.join(test_dir, "wal.log")

    # Test with non-existent WAL file
    {:ok, result} = KVStore.Storage.WAL.replay(wal_path, fn _entry -> :ok end)
    assert result == :no_wal
  end

  test "replay WAL with operations", %{test_dir: test_dir} do
    {:ok, wal} = KVStore.Storage.WAL.open(test_dir, :no_sync)

    # Write some operations
    {:ok, wal2} = KVStore.Storage.WAL.write_put(wal, "key1", "value1", 1)
    {:ok, wal3} = KVStore.Storage.WAL.write_delete(wal2, "key2", 2)
    {:ok, wal4} = KVStore.Storage.WAL.write_checkpoint(wal3, %{timestamp: 123_456})

    KVStore.Storage.WAL.close(wal4)

    # Replay the WAL
    _entries = []

    {:ok, result} =
      KVStore.Storage.WAL.replay(wal.path, fn entry ->
        send(self(), {:replayed, entry})
        :ok
      end)

    assert result.entries == 3
    assert result.errors == 0

    # Collect replayed entries
    replayed_entries =
      Enum.map(1..3, fn _ ->
        receive do
          {:replayed, entry} -> entry
        after
          1000 ->
            flunk("Timeout waiting for replayed entry")
        end
      end)

    # Verify the entries
    entry_types = KVStore.Storage.WAL.entry_types()

    assert length(replayed_entries) == 3

    put_entry = Enum.find(replayed_entries, fn entry -> entry.type == entry_types.put end)
    assert put_entry != nil
    assert put_entry.data.key == "key1"
    assert put_entry.data.value == "value1"
    assert put_entry.data.transaction_id == 1

    delete_entry = Enum.find(replayed_entries, fn entry -> entry.type == entry_types.delete end)
    assert delete_entry != nil
    assert delete_entry.data.key == "key2"
    assert delete_entry.data.transaction_id == 2

    checkpoint_entry =
      Enum.find(replayed_entries, fn entry -> entry.type == entry_types.checkpoint end)

    assert checkpoint_entry != nil
    assert checkpoint_entry.data.timestamp == 123_456
  end

  test "replay handles callback errors gracefully", %{test_dir: test_dir} do
    {:ok, wal} = KVStore.Storage.WAL.open(test_dir, :no_sync)

    # Write some operations
    {:ok, wal2} = KVStore.Storage.WAL.write_put(wal, "key1", "value1", 1)
    {:ok, wal3} = KVStore.Storage.WAL.write_put(wal2, "key2", "value2", 2)

    KVStore.Storage.WAL.close(wal3)

    # Replay with a callback that fails on the second entry
    # Use a simpler approach with Agent for state
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    {:ok, result} =
      KVStore.Storage.WAL.replay(wal.path, fn _entry ->
        count = Agent.get_and_update(counter, fn state -> {state + 1, state + 1} end)

        if count == 2 do
          {:error, :simulated_failure}
        else
          :ok
        end
      end)

    Agent.stop(counter)

    # First entry succeeded, second entry failed
    assert result.entries == 1
    # Second entry failed
    assert result.errors == 1
  end

  test "WAL with sync policies", %{test_dir: test_dir} do
    # Test sync_on_write policy
    {:ok, wal_sync} = KVStore.Storage.WAL.open(test_dir, :sync_on_write)
    {:ok, _updated_wal} = KVStore.Storage.WAL.write_put(wal_sync, "key", "value", 1)
    KVStore.Storage.WAL.close(wal_sync)

    # Test no_sync policy - create a separate WAL file
    wal_path_no_sync = Path.join(test_dir, "wal_no_sync.log")

    # Create a custom WAL for no_sync testing
    case :file.open(wal_path_no_sync, [:raw, :binary, :append, :read]) do
      {:ok, file} ->
        {:ok, offset} = :file.position(file, :eof)

        wal_no_sync = %KVStore.Storage.WAL{
          file: file,
          path: wal_path_no_sync,
          offset: offset,
          sync_policy: :no_sync
        }

        {:ok, _updated_wal} = KVStore.Storage.WAL.write_put(wal_no_sync, "key", "value", 1)
        KVStore.Storage.WAL.close(wal_no_sync)

        # Both should have created files
        assert File.exists?(wal_sync.path)
        assert File.exists?(wal_path_no_sync)

      {:error, reason} ->
        flunk("Failed to create no_sync WAL file: #{inspect(reason)}")
    end
  end

  test "truncate WAL", %{test_dir: test_dir} do
    {:ok, wal} = KVStore.Storage.WAL.open(test_dir, :no_sync)

    # Write some data
    {:ok, updated_wal} = KVStore.Storage.WAL.write_put(wal, "key", "value", 1)
    KVStore.Storage.WAL.close(updated_wal)

    # Verify file has content
    {:ok, stat_before} = File.stat(wal.path)
    assert stat_before.size > 0

    # Truncate
    :ok = KVStore.Storage.WAL.truncate(wal.path)

    # Verify file is empty
    {:ok, stat_after} = File.stat(wal.path)
    assert stat_after.size == 0
  end

  test "WAL entry types", %{test_dir: _test_dir} do
    entry_types = KVStore.Storage.WAL.entry_types()

    assert entry_types.put == 0x01
    assert entry_types.delete == 0x02
    assert entry_types.batch_start == 0x03
    assert entry_types.batch_end == 0x04
    assert entry_types.checkpoint == 0x05
  end

  test "WAL handles large values", %{test_dir: test_dir} do
    {:ok, wal} = KVStore.Storage.WAL.open(test_dir, :no_sync)

    # Create a large value (1MB)
    large_value = String.duplicate("x", 1024 * 1024)

    {:ok, updated_wal} = KVStore.Storage.WAL.write_put(wal, "large_key", large_value, 1)

    # Should be larger than the value
    assert updated_wal.offset > 1024 * 1024

    KVStore.Storage.WAL.close(updated_wal)

    # Verify we can read it back
    {:ok, result} =
      KVStore.Storage.WAL.replay(wal.path, fn entry ->
        assert entry.data.key == "large_key"
        assert entry.data.value == large_value
        :ok
      end)

    assert result.entries == 1
    assert result.errors == 0
  end

  test "WAL handles complex data structures", %{test_dir: test_dir} do
    {:ok, wal} = KVStore.Storage.WAL.open(test_dir, :no_sync)

    # Test with complex Erlang terms
    complex_key = {:tuple, "string", 123, [1, 2, 3], %{map: "value"}}

    complex_value = %{
      nested: %{
        list: [1, 2, 3, {:nested_tuple, "value"}],
        binary: <<1, 2, 3, 4>>,
        atom: :test_atom
      }
    }

    {:ok, updated_wal} = KVStore.Storage.WAL.write_put(wal, complex_key, complex_value, 1)

    KVStore.Storage.WAL.close(updated_wal)

    # Verify we can read it back correctly
    {:ok, result} =
      KVStore.Storage.WAL.replay(wal.path, fn entry ->
        assert entry.data.key == complex_key
        assert entry.data.value == complex_value
        :ok
      end)

    assert result.entries == 1
    assert result.errors == 0
  end
end
