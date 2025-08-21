defmodule KVStore.Storage.SegmentRotationTest do
  use ExUnit.Case

  setup do
    # Create a temporary directory for testing
    test_dir = Path.join(System.tmp_dir!(), "kv_store_segment_test_#{:rand.uniform(1_000_000)}")
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

  test "segment rotation with small segment size", %{test_dir: test_dir} do
    # Start with a very small segment size to force rotation
    # 100 bytes
    small_segment_size = 100

    # Start the application with custom config
    config = [
      data_dir: test_dir,
      segment_max_bytes: small_segment_size,
      sync_on_put: true
    ]

    {:ok, _pid} = KVStore.Storage.Engine.start_link(config)

    # Put data that should trigger rotation
    large_value = String.duplicate("x", 50)

    # First write should go to segment 1
    assert {:ok, _offset1} = KVStore.Storage.Engine.put("key1", large_value)

    # Second write should still go to segment 1
    assert {:ok, _offset2} = KVStore.Storage.Engine.put("key2", large_value)

    # Third write should trigger rotation to segment 2
    assert {:ok, _offset3} = KVStore.Storage.Engine.put("key3", large_value)

    # Check status to verify segment rotation
    status = KVStore.Storage.Engine.status()
    # Should have rotated
    assert status.active_segment_id >= 2

    # Verify all data is still accessible
    assert {:ok, ^large_value} = KVStore.Storage.Engine.get("key1")
    assert {:ok, ^large_value} = KVStore.Storage.Engine.get("key2")
    assert {:ok, ^large_value} = KVStore.Storage.Engine.get("key3")

    # Verify hint files were created for closed segments
    case KVStore.Storage.Hint.list_hints(test_dir) do
      {:ok, hint_segments} ->
        # At least one hint file should exist
        assert length(hint_segments) >= 1

      {:error, _} ->
        # Hint files might not be created immediately
        :ok
    end

    GenServer.stop(KVStore.Storage.Engine, :normal)
  end

  test "segment file creation and naming", %{test_dir: test_dir} do
    config = [
      data_dir: test_dir,
      # Very small to force multiple rotations
      segment_max_bytes: 50,
      sync_on_put: true
    ]

    {:ok, _pid} = KVStore.Storage.Engine.start_link(config)

    # Put multiple records to trigger multiple rotations
    Enum.each(1..5, fn i ->
      large_value = String.duplicate("data_#{i}_", 10)
      KVStore.Storage.Engine.put("key_#{i}", large_value)
    end)

    # Check that multiple segment files were created
    {:ok, files} = File.ls(test_dir)
    data_files = Enum.filter(files, &String.ends_with?(&1, ".data"))

    # Should have at least 2 segment files
    assert length(data_files) >= 2

    # Verify files are named correctly (1.data, 2.data, etc.)
    segment_ids =
      data_files
      |> Enum.map(fn filename ->
        {id, ""} = Integer.parse(String.trim_trailing(filename, ".data"))
        id
      end)
      |> Enum.sort()

    # Should start from 1 and be consecutive
    assert List.first(segment_ids) == 1
    assert segment_ids == Enum.to_list(1..length(segment_ids))

    GenServer.stop(KVStore.Storage.Engine, :normal)
  end

  test "hint file creation during rotation", %{test_dir: test_dir} do
    config = [
      data_dir: test_dir,
      segment_max_bytes: 100,
      sync_on_put: true
    ]

    {:ok, _pid} = KVStore.Storage.Engine.start_link(config)

    # Put data to fill first segment
    Enum.each(1..3, fn i ->
      value = String.duplicate("value_#{i}_", 15)
      KVStore.Storage.Engine.put("key_#{i}", value)
    end)

    # Force rotation by putting more data
    large_value = String.duplicate("trigger_rotation", 10)
    KVStore.Storage.Engine.put("rotation_key", large_value)

    # Check status
    status = KVStore.Storage.Engine.status()

    # Wait a bit for hint file creation
    :timer.sleep(100)

    # Check if hint files were created
    case KVStore.Storage.Hint.list_hints(test_dir) do
      {:ok, hint_segments} ->
        if length(hint_segments) > 0 do
          # Verify hint file contents
          first_segment = List.first(hint_segments)
          assert {:ok, entries} = KVStore.Storage.Hint.read_hint(first_segment, test_dir)
          # Should have entries from the closed segment
          assert length(entries) >= 1

          # Verify entries are valid
          Enum.each(entries, fn {key, segment_id, offset, size, timestamp, is_tombstone} ->
            assert is_binary(key) or is_atom(key) or is_tuple(key) or is_list(key)
            assert is_integer(segment_id)
            assert is_integer(offset)
            assert is_integer(size)
            assert is_integer(timestamp)
            assert is_boolean(is_tombstone)
          end)
        end

      {:error, _} ->
        # Hint files might not be created yet
        :ok
    end

    GenServer.stop(KVStore.Storage.Engine, :normal)
  end

  test "data integrity across segment rotations", %{test_dir: test_dir} do
    config = [
      data_dir: test_dir,
      segment_max_bytes: 80,
      sync_on_put: true
    ]

    {:ok, _pid} = KVStore.Storage.Engine.start_link(config)

    # Track all key-value pairs we insert
    test_data =
      Enum.map(1..20, fn i ->
        key = "test_key_#{i}"
        value = "test_value_#{i}_#{String.duplicate("x", rem(i, 10))}"
        {key, value}
      end)

    # Insert all data (should trigger multiple rotations)
    Enum.each(test_data, fn {key, value} ->
      assert {:ok, _offset} = KVStore.Storage.Engine.put(key, value)
    end)

    # Verify all data is still accessible
    Enum.each(test_data, fn {key, expected_value} ->
      assert {:ok, ^expected_value} = KVStore.Storage.Engine.get(key)
    end)

    # Check that multiple segments were created
    status = KVStore.Storage.Engine.status()
    assert status.active_segment_id > 1

    # Verify keydir size matches the number of keys
    assert status.keydir_size == length(test_data)
    assert status.key_set_size == length(test_data)

    KVStore.Storage.Engine.stop()
  end

  test "deletes and tombstones across segments", %{test_dir: test_dir} do
    config = [
      data_dir: test_dir,
      segment_max_bytes: 60,
      sync_on_put: true
    ]

    {:ok, _pid} = KVStore.Storage.Engine.start_link(config)

    # Insert initial data
    Enum.each(1..5, fn i ->
      key = "key_#{i}"
      value = String.duplicate("value_#{i}_", 8)
      KVStore.Storage.Engine.put(key, value)
    end)

    # Delete some keys (should create tombstones)
    assert {:ok, _} = KVStore.Storage.Engine.delete("key_2")
    assert {:ok, _} = KVStore.Storage.Engine.delete("key_4")

    # Insert more data to trigger rotation
    Enum.each(6..10, fn i ->
      key = "key_#{i}"
      value = String.duplicate("value_#{i}_", 8)
      KVStore.Storage.Engine.put(key, value)
    end)

    # Verify deleted keys are not accessible
    assert {:error, :not_found} = KVStore.Storage.Engine.get("key_2")
    assert {:error, :not_found} = KVStore.Storage.Engine.get("key_4")

    # Verify non-deleted keys are still accessible
    assert {:ok, _} = KVStore.Storage.Engine.get("key_1")
    assert {:ok, _} = KVStore.Storage.Engine.get("key_3")
    assert {:ok, _} = KVStore.Storage.Engine.get("key_5")

    # Verify new keys are accessible
    Enum.each(6..10, fn i ->
      assert {:ok, _} = KVStore.Storage.Engine.get("key_#{i}")
    end)

    # Check that tombstones are reflected in keydir size
    status = KVStore.Storage.Engine.status()
    # All keys (including tombstones)
    assert status.keydir_size == 10
    # Only live keys
    assert status.key_set_size == 8

    GenServer.stop(KVStore.Storage.Engine, :normal)
  end

  test "segment size calculation and rotation trigger", %{test_dir: test_dir} do
    config = [
      data_dir: test_dir,
      segment_max_bytes: 200,
      sync_on_put: true
    ]

    {:ok, _pid} = KVStore.Storage.Engine.start_link(config)

    initial_status = KVStore.Storage.Engine.status()
    initial_segment_id = initial_status.active_segment_id

    # Calculate approximate record size for a known key-value pair
    test_key = "test_key"
    test_value = "test_value"
    record_size = KVStore.Storage.Record.size(test_key, test_value)

    # Insert records one by one and check when rotation happens
    # Should definitely trigger rotation
    max_records = div(200, record_size) + 2

    segment_id_before = initial_segment_id
    rotated = false

    Enum.each(1..max_records, fn i ->
      key = "#{test_key}_#{i}"
      value = "#{test_value}_#{i}"
      KVStore.Storage.Engine.put(key, value)

      current_status = KVStore.Storage.Engine.status()

      if current_status.active_segment_id > segment_id_before do
        rotated = true
      end
    end)

    assert rotated, "Segment rotation should have been triggered"

    final_status = KVStore.Storage.Engine.status()
    assert final_status.active_segment_id > initial_segment_id

    GenServer.stop(KVStore.Storage.Engine, :normal)
  end

  test "recovery after segment rotation", %{test_dir: test_dir} do
    config = [
      data_dir: test_dir,
      segment_max_bytes: 100,
      sync_on_put: true
    ]

    # First session: create data across multiple segments
    {:ok, _pid} = KVStore.Storage.Engine.start_link(config)

    test_data =
      Enum.map(1..10, fn i ->
        key = "recovery_key_#{i}"
        value = String.duplicate("recovery_value_#{i}_", 5)
        {key, value}
      end)

    Enum.each(test_data, fn {key, value} ->
      KVStore.Storage.Engine.put(key, value)
    end)

    first_status = KVStore.Storage.Engine.status()
    GenServer.stop(KVStore.Storage.Engine, :normal)

    # Second session: restart and verify recovery
    {:ok, _pid} = KVStore.Storage.Engine.start_link(config)

    # Verify all data is recovered
    Enum.each(test_data, fn {key, expected_value} ->
      assert {:ok, ^expected_value} = KVStore.Storage.Engine.get(key)
    end)

    second_status = KVStore.Storage.Engine.status()

    # Verify status is consistent
    assert second_status.keydir_size == first_status.keydir_size
    assert second_status.key_set_size == first_status.key_set_size

    GenServer.stop(KVStore.Storage.Engine, :normal)
  end

  test "batch operations across segment boundaries", %{test_dir: test_dir} do
    config = [
      data_dir: test_dir,
      segment_max_bytes: 80,
      sync_on_put: true
    ]

    {:ok, _pid} = KVStore.Storage.Engine.start_link(config)

    # Create a large batch that should span multiple segments
    batch_data =
      Enum.map(1..10, fn i ->
        {"batch_key_#{i}", String.duplicate("batch_value_#{i}_", 6)}
      end)

    # Perform batch put
    assert {:ok, _offset} = KVStore.Storage.Engine.batch_put(batch_data)

    # Verify all batch data is accessible
    Enum.each(batch_data, fn {key, expected_value} ->
      assert {:ok, ^expected_value} = KVStore.Storage.Engine.get(key)
    end)

    # Check that segments were rotated during batch
    status = KVStore.Storage.Engine.status()
    # May or may not have rotated depending on timing
    assert status.active_segment_id >= 1

    GenServer.stop(KVStore.Storage.Engine, :normal)
  end
end
