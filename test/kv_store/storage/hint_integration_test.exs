defmodule KVStore.Storage.HintIntegrationTest do
  use ExUnit.Case

  setup do
    # Create a temporary directory for testing
    test_dir =
      Path.join(System.tmp_dir!(), "kv_store_hint_integration_test_#{:rand.uniform(1_000_000)}")

    File.rm_rf!(test_dir)
    File.mkdir_p!(test_dir)

    # Stop any existing engine
    try do
      GenServer.stop(KVStore.Storage.Engine, :normal)
    catch
      :exit, _ -> :ok
    end

    on_exit(fn ->
      try do
        GenServer.stop(KVStore.Storage.Engine, :normal)
      catch
        :exit, _ -> :ok
      end

      File.rm_rf!(test_dir)
    end)

    {:ok, test_dir: test_dir}
  end

  test "hint file creation on engine shutdown", %{test_dir: test_dir} do
    config = [
      data_dir: test_dir,
      # Large segment to avoid rotation
      segment_max_bytes: 1024 * 1024,
      sync_on_put: true
    ]

    # Start engine and add some data
    {:ok, pid} = KVStore.Storage.Engine.start_link(config)

    test_data = [
      {"shutdown_key_1", "shutdown_value_1"},
      {"shutdown_key_2", "shutdown_value_2"},
      {"shutdown_key_3", "shutdown_value_3"}
    ]

    Enum.each(test_data, fn {key, value} ->
      KVStore.Storage.Engine.put(key, value)
    end)

    # Stop the engine (should create hint file)
    GenServer.stop(pid, :normal)

    # Check that hint file was created
    assert {:ok, hint_segments} = KVStore.Storage.Hint.list_hints(test_dir)
    assert length(hint_segments) >= 1

    # Verify hint file contents
    first_segment = List.first(hint_segments)
    assert {:ok, entries} = KVStore.Storage.Hint.read_hint(first_segment, test_dir)

    # Should have entries for our test data
    assert length(entries) == length(test_data)

    # Verify entry contents
    entry_keys = Enum.map(entries, fn {key, _, _, _, _, _} -> key end)
    expected_keys = Enum.map(test_data, fn {key, _} -> key end)
    assert Enum.sort(entry_keys) == Enum.sort(expected_keys)
  end

  test "hint file loading on engine startup", %{test_dir: test_dir} do
    config = [
      data_dir: test_dir,
      # Small segment to force rotation
      segment_max_bytes: 200,
      sync_on_put: true
    ]

    # First session: create data and hint files
    {:ok, pid1} = KVStore.Storage.Engine.start_link(config)

    test_data =
      Enum.map(1..8, fn i ->
        {"startup_key_#{i}", "startup_value_#{i}_#{String.duplicate("x", 10)}"}
      end)

    Enum.each(test_data, fn {key, value} ->
      KVStore.Storage.Engine.put(key, value)
    end)

    first_status = KVStore.Storage.Engine.status()
    GenServer.stop(pid1, :normal)

    # Verify hint files were created
    assert {:ok, hint_segments} = KVStore.Storage.Hint.list_hints(test_dir)
    assert length(hint_segments) >= 1

    # Second session: start engine and verify data recovery
    {:ok, pid2} = KVStore.Storage.Engine.start_link(config)

    # Verify all data is recovered
    Enum.each(test_data, fn {key, expected_value} ->
      assert {:ok, ^expected_value} = KVStore.Storage.Engine.get(key)
    end)

    # Verify status consistency
    second_status = KVStore.Storage.Engine.status()
    assert second_status.keydir_size == first_status.keydir_size
    assert second_status.key_set_size == first_status.key_set_size

    GenServer.stop(pid2, :normal)
  end

  test "partial hint file recovery with missing files", %{test_dir: test_dir} do
    config = [
      data_dir: test_dir,
      segment_max_bytes: 100,
      sync_on_put: true
    ]

    # Create engine and force multiple segment rotations
    {:ok, pid} = KVStore.Storage.Engine.start_link(config)

    # Add data across multiple segments
    Enum.each(1..15, fn i ->
      key = "partial_key_#{i}"
      value = String.duplicate("partial_value_#{i}_", 5)
      KVStore.Storage.Engine.put(key, value)
    end)

    status_before = KVStore.Storage.Engine.status()
    GenServer.stop(pid, :normal)

    # Verify multiple hint files were created
    assert {:ok, hint_segments} = KVStore.Storage.Hint.list_hints(test_dir)
    assert length(hint_segments) >= 2

    # Delete one of the hint files to simulate corruption/loss
    deleted_segment = List.first(hint_segments)
    KVStore.Storage.Hint.delete_hint(deleted_segment, test_dir)

    # Start engine again (should handle missing hint file gracefully)
    {:ok, pid2} = KVStore.Storage.Engine.start_link(config)

    # Some data should still be recovered from remaining hint files
    status_after = KVStore.Storage.Engine.status()

    # Should have recovered at least some data
    # Some data lost
    assert status_after.keydir_size < status_before.keydir_size
    # But not all data lost
    assert status_after.keydir_size > 0

    GenServer.stop(pid2, :normal)
  end

  test "hint file recovery with tombstones", %{test_dir: test_dir} do
    config = [
      data_dir: test_dir,
      segment_max_bytes: 150,
      sync_on_put: true
    ]

    # First session: create data and deletions
    {:ok, pid1} = KVStore.Storage.Engine.start_link(config)

    # Add initial data
    Enum.each(1..10, fn i ->
      key = "tombstone_key_#{i}"
      value = String.duplicate("tombstone_value_#{i}_", 6)
      KVStore.Storage.Engine.put(key, value)
    end)

    # Delete some keys
    KVStore.Storage.Engine.delete("tombstone_key_2")
    KVStore.Storage.Engine.delete("tombstone_key_4")
    KVStore.Storage.Engine.delete("tombstone_key_6")

    # Add more data to trigger rotation
    Enum.each(11..15, fn i ->
      key = "tombstone_key_#{i}"
      value = String.duplicate("tombstone_value_#{i}_", 6)
      KVStore.Storage.Engine.put(key, value)
    end)

    status_before = KVStore.Storage.Engine.status()
    GenServer.stop(pid1, :normal)

    # Second session: verify tombstone recovery
    {:ok, pid2} = KVStore.Storage.Engine.start_link(config)

    # Verify deleted keys are still not accessible
    assert {:error, :not_found} = KVStore.Storage.Engine.get("tombstone_key_2")
    assert {:error, :not_found} = KVStore.Storage.Engine.get("tombstone_key_4")
    assert {:error, :not_found} = KVStore.Storage.Engine.get("tombstone_key_6")

    # Verify non-deleted keys are accessible
    assert {:ok, _} = KVStore.Storage.Engine.get("tombstone_key_1")
    assert {:ok, _} = KVStore.Storage.Engine.get("tombstone_key_3")
    assert {:ok, _} = KVStore.Storage.Engine.get("tombstone_key_5")

    # Verify status consistency
    status_after = KVStore.Storage.Engine.status()
    # Includes tombstones
    assert status_after.keydir_size == status_before.keydir_size
    # Only live keys
    assert status_after.key_set_size == status_before.key_set_size

    GenServer.stop(pid2, :normal)
  end

  test "hint file recovery performance with large datasets", %{test_dir: test_dir} do
    config = [
      data_dir: test_dir,
      # Moderate segment size
      segment_max_bytes: 1024,
      sync_on_put: true
    ]

    # First session: create large dataset
    {:ok, pid1} = KVStore.Storage.Engine.start_link(config)

    # Create a substantial dataset (1000 keys)
    large_dataset =
      Enum.map(1..1000, fn i ->
        key = "perf_key_#{String.pad_leading("#{i}", 4, "0")}"
        value = "perf_value_#{i}_#{String.duplicate("data", 5)}"
        {key, value}
      end)

    # Measure insertion time
    {insert_time, _} =
      :timer.tc(fn ->
        Enum.each(large_dataset, fn {key, value} ->
          KVStore.Storage.Engine.put(key, value)
        end)
      end)

    first_status = KVStore.Storage.Engine.status()
    GenServer.stop(pid1, :normal)

    # Verify hint files were created
    assert {:ok, hint_segments} = KVStore.Storage.Hint.list_hints(test_dir)
    # Should have created multiple segments
    assert length(hint_segments) >= 3

    # Second session: measure recovery time
    {recovery_time, {:ok, pid2}} =
      :timer.tc(fn ->
        KVStore.Storage.Engine.start_link(config)
      end)

    # Verify recovery was successful
    second_status = KVStore.Storage.Engine.status()
    assert second_status.keydir_size == first_status.keydir_size
    assert second_status.key_set_size == first_status.key_set_size

    # Recovery should be significantly faster than insertion
    assert recovery_time < insert_time / 2,
           "Hint file recovery should be faster than initial insertion"

    # Verify random access to data after recovery
    sample_keys = Enum.take_random(large_dataset, 100)

    Enum.each(sample_keys, fn {key, expected_value} ->
      assert {:ok, ^expected_value} = KVStore.Storage.Engine.get(key)
    end)

    GenServer.stop(pid2, :normal)
  end

  test "hint file cleanup and management", %{test_dir: test_dir} do
    config = [
      data_dir: test_dir,
      segment_max_bytes: 100,
      sync_on_put: true
    ]

    {:ok, pid} = KVStore.Storage.Engine.start_link(config)

    # Create data to generate multiple segments and hint files
    Enum.each(1..20, fn i ->
      key = "cleanup_key_#{i}"
      value = String.duplicate("cleanup_value_#{i}_", 4)
      KVStore.Storage.Engine.put(key, value)
    end)

    GenServer.stop(pid, :normal)

    # Check initial hint files
    assert {:ok, initial_hints} = KVStore.Storage.Hint.list_hints(test_dir)
    initial_count = length(initial_hints)
    assert initial_count >= 2

    # Test hint file deletion
    first_hint = List.first(initial_hints)
    assert :ok = KVStore.Storage.Hint.delete_hint(first_hint, test_dir)

    # Verify deletion
    assert {:ok, remaining_hints} = KVStore.Storage.Hint.list_hints(test_dir)
    assert length(remaining_hints) == initial_count - 1
    refute first_hint in remaining_hints

    # Test hint file existence check
    assert not KVStore.Storage.Hint.hint_exists?(first_hint, test_dir)

    if length(remaining_hints) > 0 do
      second_hint = List.first(remaining_hints)
      assert KVStore.Storage.Hint.hint_exists?(second_hint, test_dir)
    end
  end

  test "hint file versioning and compatibility", %{test_dir: test_dir} do
    # Create a hint file with the current format
    segment_id = 1

    keydir_entries = [
      {"version_key_1", segment_id, 0, 50, 1_234_567_890, false},
      {"version_key_2", segment_id, 50, 60, 1_234_567_891, true}
    ]

    assert {:ok, hint_path} =
             KVStore.Storage.Hint.create_hint(segment_id, test_dir, keydir_entries)

    # Verify the hint file has correct version
    {:ok, file} = :file.open(hint_path, [:raw, :binary, :read])
    {:ok, header} = :file.read(file, 13)
    :file.close(file)

    <<_magic::32-big, version::8, _num_entries::32-big, _reserved::32>> = header
    # Current version
    assert version == 0x01

    # Read back and verify
    assert {:ok, read_entries} = KVStore.Storage.Hint.read_hint(segment_id, test_dir)
    assert length(read_entries) == 2
    assert read_entries == keydir_entries
  end

  test "concurrent access to hint files", %{test_dir: test_dir} do
    segment_id = 1

    # Create initial hint file
    initial_entries = [{"concurrent_key", segment_id, 0, 50, 1_234_567_890, false}]
    assert {:ok, _} = KVStore.Storage.Hint.create_hint(segment_id, test_dir, initial_entries)

    # Multiple processes trying to read the same hint file
    read_tasks =
      Enum.map(1..10, fn _ ->
        Task.async(fn ->
          KVStore.Storage.Hint.read_hint(segment_id, test_dir)
        end)
      end)

    # Wait for all reads to complete
    read_results = Enum.map(read_tasks, &Task.await/1)

    # All reads should succeed with the same data
    Enum.each(read_results, fn result ->
      assert {:ok, entries} = result
      assert entries == initial_entries
    end)

    # Test concurrent read/write (read while creating new hint file)
    write_task =
      Task.async(fn ->
        new_entries = [{"new_concurrent_key", 2, 100, 75, 1_234_567_900, false}]
        KVStore.Storage.Hint.create_hint(2, test_dir, new_entries)
      end)

    read_task =
      Task.async(fn ->
        # Read existing hint file while new one is being created
        KVStore.Storage.Hint.read_hint(segment_id, test_dir)
      end)

    # Both should complete successfully
    assert {:ok, _} = Task.await(write_task)
    assert {:ok, entries} = Task.await(read_task)
    assert entries == initial_entries
  end
end
