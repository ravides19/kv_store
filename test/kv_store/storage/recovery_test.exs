defmodule KVStore.Storage.RecoveryTest do
  use ExUnit.Case

  setup do
    # Create a temporary directory for testing
    test_dir = Path.join(System.tmp_dir!(), "kv_store_recovery_test_#{:rand.uniform(1_000_000)}")
    File.rm_rf!(test_dir)
    File.mkdir_p!(test_dir)

    on_exit(fn ->
      File.rm_rf!(test_dir)
    end)

    {:ok, test_dir: test_dir}
  end

  test "scan_segments finds existing segment files", %{test_dir: test_dir} do
    # Create some test segment files
    File.write!(Path.join(test_dir, "1.data"), "test data 1")
    File.write!(Path.join(test_dir, "2.data"), "test data 2")
    File.write!(Path.join(test_dir, "3.data"), "test data 3")

    {:ok, segments} = KVStore.Storage.Recovery.scan_segments(test_dir)

    assert length(segments) == 3
    assert Enum.all?(segments, & &1.exists)

    segment_ids = Enum.map(segments, & &1.id) |> Enum.sort()
    assert segment_ids == [1, 2, 3]
  end

  test "scan_segments handles empty directory", %{test_dir: test_dir} do
    {:ok, segments} = KVStore.Storage.Recovery.scan_segments(test_dir)
    assert segments == []
  end

  test "validate_segment with valid records", %{test_dir: test_dir} do
    # Create a segment with valid records
    segment_path = Path.join(test_dir, "1.data")
    {:ok, file} = :file.open(segment_path, [:raw, :binary, :write])

    # Write a valid record
    record_data = KVStore.Storage.Record.create("test_key", "test_value")
    :file.write(file, record_data)
    :file.close(file)

    segment = %{id: 1, path: segment_path, exists: true}
    {:ok, validated_segment} = KVStore.Storage.Recovery.validate_segment(segment, test_dir)

    assert validated_segment.records_count == 1
    assert validated_segment.truncated == false
  end

  test "validate_segment with corrupted records", %{test_dir: test_dir} do
    # Create a segment with corrupted data
    segment_path = Path.join(test_dir, "1.data")

    # Write a valid record first
    {:ok, file} = :file.open(segment_path, [:raw, :binary, :write])
    record_data = KVStore.Storage.Record.create("test_key", "test_value")
    :file.write(file, record_data)

    # Write some corrupted data
    :file.write(file, "corrupted data that doesn't match record format")
    :file.close(file)

    segment = %{id: 1, path: segment_path, exists: true}
    {:ok, validated_segment} = KVStore.Storage.Recovery.validate_segment(segment, test_dir)

    assert validated_segment.records_count == 1
    assert validated_segment.truncated == true
  end

  test "recover with no existing data", %{test_dir: test_dir} do
    {:ok, recovery_info, recovery_data} = KVStore.Storage.Recovery.recover(test_dir)

    assert recovery_info.segments_found == 0
    assert recovery_info.segments_valid == 0
    assert recovery_info.entries_recovered == 0
    assert recovery_info.recovery_time_ms > 0

    # Clean up
    :ets.delete(recovery_data.keydir)
    :ets.delete(recovery_data.key_set)
  end

  test "recover with valid segments and hint files", %{test_dir: test_dir} do
    # Create test data
    test_entries = [
      {"key1", "value1"},
      {"key2", "value2"},
      {"key3", "value3"}
    ]

    # Create a segment file with records
    segment_path = Path.join(test_dir, "1.data")
    {:ok, file} = :file.open(segment_path, [:raw, :binary, :write])

    offset = 0

    hint_entries =
      Enum.map(test_entries, fn {key, value} ->
        record_data = KVStore.Storage.Record.create(key, value)
        :file.write(file, record_data)
        record_size = IO.iodata_length(record_data)

        entry = {key, 1, offset, record_size - 30, :os.system_time(:millisecond), false}
        _offset = offset + record_size
        entry
      end)

    :file.close(file)

    # Create a hint file
    {:ok, _hint_path} = KVStore.Storage.Hint.create_hint(1, test_dir, hint_entries)

    # Perform recovery
    {:ok, recovery_info, recovery_data} = KVStore.Storage.Recovery.recover(test_dir)

    assert recovery_info.segments_found == 1
    assert recovery_info.segments_valid == 1
    assert recovery_info.entries_recovered == 3
    assert recovery_data.keydir_entries == 3

    # Verify the data is correctly loaded
    Enum.each(test_entries, fn {key, _value} ->
      assert :ets.lookup(recovery_data.keydir, key) != []
      assert :ets.lookup(recovery_data.key_set, key) != []
    end)

    # Clean up
    :ets.delete(recovery_data.keydir)
    :ets.delete(recovery_data.key_set)
  end

  test "recover handles tombstones correctly", %{test_dir: test_dir} do
    # Create a segment with regular entries and tombstones
    segment_path = Path.join(test_dir, "1.data")
    {:ok, file} = :file.open(segment_path, [:raw, :binary, :write])

    # Write regular record
    record1 = KVStore.Storage.Record.create("key1", "value1")
    :file.write(file, record1)

    # Write tombstone record
    tombstone = KVStore.Storage.Record.create_tombstone("key2")
    :file.write(file, tombstone)

    :file.close(file)

    # Create hint entries
    hint_entries = [
      {"key1", 1, 0, IO.iodata_length(record1) - 30, :os.system_time(:millisecond), false},
      {"key2", 1, IO.iodata_length(record1), IO.iodata_length(tombstone) - 30,
       :os.system_time(:millisecond), true}
    ]

    {:ok, _hint_path} = KVStore.Storage.Hint.create_hint(1, test_dir, hint_entries)

    # Perform recovery
    {:ok, recovery_info, recovery_data} = KVStore.Storage.Recovery.recover(test_dir)

    assert recovery_info.entries_recovered == 2
    assert recovery_info.tombstones_found == 1

    # Regular key should be in both tables
    assert :ets.lookup(recovery_data.keydir, "key1") != []
    assert :ets.lookup(recovery_data.key_set, "key1") != []

    # Tombstone key should be in keydir but not key_set
    assert :ets.lookup(recovery_data.keydir, "key2") != []
    assert :ets.lookup(recovery_data.key_set, "key2") == []

    # Clean up
    :ets.delete(recovery_data.keydir)
    :ets.delete(recovery_data.key_set)
  end

  test "verify_integrity with valid segments", %{test_dir: test_dir} do
    # Create a segment with valid records
    segment_path = Path.join(test_dir, "1.data")
    {:ok, file} = :file.open(segment_path, [:raw, :binary, :write])

    # Write multiple valid records
    Enum.each(1..5, fn i ->
      record_data = KVStore.Storage.Record.create("key_#{i}", "value_#{i}")
      :file.write(file, record_data)
    end)

    :file.close(file)

    {:ok, result} = KVStore.Storage.Recovery.verify_integrity(test_dir)
    assert result == :all_valid
  end

  test "verify_integrity detects corrupted segments", %{test_dir: test_dir} do
    # Create a segment with valid and invalid records
    segment_path = Path.join(test_dir, "1.data")
    File.write!(segment_path, "this is not a valid record format")

    {:error, {:integrity_check_failed, failed_segments}} =
      KVStore.Storage.Recovery.verify_integrity(test_dir)

    assert length(failed_segments) == 1
    # segment id
    assert elem(hd(failed_segments), 0) == 1
    # status
    assert elem(hd(failed_segments), 1) == :error
  end

  test "recovery with fallback to data file scanning", %{test_dir: test_dir} do
    # Create a segment file without hint file
    segment_path = Path.join(test_dir, "1.data")
    {:ok, file} = :file.open(segment_path, [:raw, :binary, :write])

    # Write some records
    test_entries = [
      {"fallback_key1", "fallback_value1"},
      {"fallback_key2", "fallback_value2"}
    ]

    Enum.each(test_entries, fn {key, value} ->
      record_data = KVStore.Storage.Record.create(key, value)
      :file.write(file, record_data)
    end)

    :file.close(file)

    # Don't create a hint file - force fallback to data file scanning

    # Perform recovery
    {:ok, recovery_info, recovery_data} = KVStore.Storage.Recovery.recover(test_dir)

    assert recovery_info.segments_found == 1
    assert recovery_info.segments_valid == 1
    assert recovery_info.entries_recovered == 2

    # Verify the data is correctly loaded
    Enum.each(test_entries, fn {key, _value} ->
      assert :ets.lookup(recovery_data.keydir, key) != []
      assert :ets.lookup(recovery_data.key_set, key) != []
    end)

    # Clean up
    :ets.delete(recovery_data.keydir)
    :ets.delete(recovery_data.key_set)
  end

  test "recovery performance with large dataset", %{test_dir: test_dir} do
    # Create a segment with many records
    segment_path = Path.join(test_dir, "1.data")
    {:ok, file} = :file.open(segment_path, [:raw, :binary, :write])

    # Write 1000 records
    num_records = 1000

    hint_entries =
      Enum.reduce(1..num_records, {[], 0}, fn i, {entries, offset} ->
        key = "perf_key_#{i}"
        value = "perf_value_#{i}"
        record_data = KVStore.Storage.Record.create(key, value)
        :file.write(file, record_data)

        record_size = IO.iodata_length(record_data)
        entry = {key, 1, offset, record_size - 30, :os.system_time(:millisecond), false}
        {[entry | entries], offset + record_size}
      end)
      |> elem(0)
      |> Enum.reverse()

    :file.close(file)

    # Create hint file
    {:ok, _hint_path} = KVStore.Storage.Hint.create_hint(1, test_dir, hint_entries)

    # Time the recovery
    start_time = :os.system_time(:millisecond)
    {:ok, recovery_info, recovery_data} = KVStore.Storage.Recovery.recover(test_dir)
    end_time = :os.system_time(:millisecond)

    assert recovery_info.entries_recovered == num_records
    # Should complete in under 5 seconds
    assert recovery_info.recovery_time_ms < 5000
    assert end_time - start_time < 5000

    # Clean up
    :ets.delete(recovery_data.keydir)
    :ets.delete(recovery_data.key_set)
  end
end
