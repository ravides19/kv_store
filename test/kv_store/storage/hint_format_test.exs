defmodule KVStore.Storage.HintFormatTest do
  use ExUnit.Case

  setup do
    # Create a temporary directory for testing
    test_dir =
      Path.join(System.tmp_dir!(), "kv_store_hint_format_test_#{:rand.uniform(1_000_000)}")

    File.mkdir_p!(test_dir)

    on_exit(fn ->
      File.rm_rf!(test_dir)
    end)

    {:ok, test_dir: test_dir}
  end

  test "hint file header format validation", %{test_dir: test_dir} do
    segment_id = 1
    keydir_entries = [{"test_key", segment_id, 0, 50, 1_234_567_890, false}]

    # Create hint file
    assert {:ok, hint_path} =
             KVStore.Storage.Hint.create_hint(segment_id, test_dir, keydir_entries)

    # Read the file manually and verify header format
    {:ok, file} = :file.open(hint_path, [:raw, :binary, :read])
    {:ok, header} = :file.read(file, 13)
    :file.close(file)

    # Parse header manually
    <<magic::32-big, version::8, num_entries::32-big, reserved::32>> = header

    assert magic == 0xCAFEBABE
    assert version == 0x01
    assert num_entries == 1
    assert reserved == 0
  end

  test "hint file entry format validation", %{test_dir: test_dir} do
    segment_id = 1
    key = "test_key"
    keydir_entries = [{key, segment_id, 100, 50, 1_234_567_890, false}]

    # Create hint file
    assert {:ok, hint_path} =
             KVStore.Storage.Hint.create_hint(segment_id, test_dir, keydir_entries)

    # Read the file manually and verify entry format
    {:ok, file} = :file.open(hint_path, [:raw, :binary, :read])
    # Skip header
    {:ok, _header} = :file.read(file, 13)
    # Fixed part of entry
    {:ok, entry_fixed} = :file.read(file, 27)

    <<key_len::16-big, seg_id::32-big, offset::64-big, size::32-big, timestamp::64-big, flags::8>> =
      entry_fixed

    {:ok, key_binary} = :file.read(file, key_len)
    :file.close(file)

    assert key_len == byte_size(key)
    assert seg_id == segment_id
    assert offset == 100
    assert size == 50
    assert timestamp == 1_234_567_890
    # Not a tombstone
    assert flags == 0
    assert key_binary == key
  end

  test "tombstone entry format validation", %{test_dir: test_dir} do
    segment_id = 1
    key = "deleted_key"
    keydir_entries = [{key, segment_id, 200, 0, 1_234_567_890, true}]

    # Create hint file
    assert {:ok, hint_path} =
             KVStore.Storage.Hint.create_hint(segment_id, test_dir, keydir_entries)

    # Read and verify tombstone flag
    assert {:ok, [read_entry]} = KVStore.Storage.Hint.read_hint(segment_id, test_dir)
    {read_key, read_segment_id, read_offset, read_size, read_timestamp, is_tombstone} = read_entry

    assert read_key == key
    assert read_segment_id == segment_id
    assert read_offset == 200
    assert read_size == 0
    assert read_timestamp == 1_234_567_890
    assert is_tombstone == true
  end

  test "large number of entries", %{test_dir: test_dir} do
    segment_id = 1
    # Create 1000 entries
    keydir_entries =
      Enum.map(1..1000, fn i ->
        {"key_#{i}", segment_id, i * 100, 50, 1_234_567_890 + i, rem(i, 10) == 0}
      end)

    # Create hint file
    assert {:ok, hint_path} =
             KVStore.Storage.Hint.create_hint(segment_id, test_dir, keydir_entries)

    # Verify file was created
    assert File.exists?(hint_path)
    file_size = File.stat!(hint_path).size

    # Header is 13 bytes, each entry has 27 bytes + key length
    # Keys are "key_X" so lengths vary from 5-8 bytes
    # Minimum expected size
    expected_min_size = 13 + 1000 * (27 + 5)
    assert file_size >= expected_min_size

    # Read and verify all entries
    assert {:ok, read_entries} = KVStore.Storage.Hint.read_hint(segment_id, test_dir)
    assert length(read_entries) == 1000

    # Verify order is preserved
    Enum.zip(keydir_entries, read_entries)
    |> Enum.each(fn {original, read} ->
      assert original == read
    end)
  end

  test "various key types and sizes", %{test_dir: test_dir} do
    segment_id = 1

    keydir_entries = [
      # Empty key
      {"", segment_id, 0, 10, 1_234_567_890, false},
      # Single character
      {"a", segment_id, 10, 10, 1_234_567_891, false},
      # Long string key
      {String.duplicate("x", 1000), segment_id, 20, 10, 1_234_567_892, false},
      # Binary key with special characters
      {"key\0with\nnull\tand\rspecial", segment_id, 30, 10, 1_234_567_893, false},
      # Unicode key
      {"🔑_ключ_钥匙", segment_id, 40, 10, 1_234_567_894, false},
      # Complex term as key
      {%{nested: [1, 2, {3, 4}]}, segment_id, 50, 10, 1_234_567_895, false},
      # Atom key
      {:atom_key, segment_id, 60, 10, 1_234_567_896, false}
    ]

    # Create hint file
    assert {:ok, hint_path} =
             KVStore.Storage.Hint.create_hint(segment_id, test_dir, keydir_entries)

    # Read and verify all entries
    assert {:ok, read_entries} = KVStore.Storage.Hint.read_hint(segment_id, test_dir)
    assert length(read_entries) == length(keydir_entries)

    # Verify all key types are preserved correctly
    Enum.zip(keydir_entries, read_entries)
    |> Enum.each(fn {original, read} ->
      assert original == read
    end)
  end

  test "corrupted hint file header handling", %{test_dir: test_dir} do
    hint_path = KVStore.Storage.Hint.hint_path(1, test_dir)

    # Create file with invalid magic number
    {:ok, file} = :file.open(hint_path, [:raw, :binary, :write])
    invalid_header = <<0xDEADBEEF::32-big, 0x01::8, 1::32-big, 0::32>>
    :file.write(file, invalid_header)
    :file.close(file)

    # Should return error for invalid magic
    assert {:error, {:invalid_magic, 0xDEADBEEF}} =
             KVStore.Storage.Hint.read_hint(1, test_dir)
  end

  test "truncated hint file handling", %{test_dir: test_dir} do
    hint_path = KVStore.Storage.Hint.hint_path(1, test_dir)

    # Create file with incomplete header (only 10 bytes instead of 13)
    {:ok, file} = :file.open(hint_path, [:raw, :binary, :write])
    # Missing reserved field
    incomplete_header = <<0xCAFEBABE::32-big, 0x01::8, 1::32-big>>
    :file.write(file, incomplete_header)
    :file.close(file)

    # Should return error for incomplete header
    assert {:error, {:incomplete_header, 9}} =
             KVStore.Storage.Hint.read_hint(1, test_dir)
  end

  test "hint file with incomplete entry", %{test_dir: test_dir} do
    hint_path = KVStore.Storage.Hint.hint_path(1, test_dir)

    # Create file with valid header but incomplete entry
    {:ok, file} = :file.open(hint_path, [:raw, :binary, :write])
    valid_header = <<0xCAFEBABE::32-big, 0x01::8, 1::32-big, 0::32>>
    # Missing size, timestamp, flags, and key
    incomplete_entry = <<5::16-big, 1::32-big, 100::64-big>>
    :file.write(file, valid_header)
    :file.write(file, incomplete_entry)
    :file.close(file)

    # Should return error for incomplete entry
    assert {:error, _} = KVStore.Storage.Hint.read_hint(1, test_dir)
  end

  test "empty hint file (zero entries)", %{test_dir: test_dir} do
    segment_id = 1

    # Create hint file with no entries
    assert {:ok, hint_path} =
             KVStore.Storage.Hint.create_hint(segment_id, test_dir, [])

    # Verify file was created with correct header
    assert File.exists?(hint_path)
    # Just the header
    assert File.stat!(hint_path).size == 13

    # Should read successfully with empty list
    assert {:ok, []} = KVStore.Storage.Hint.read_hint(segment_id, test_dir)
  end

  test "concurrent hint file operations", %{test_dir: test_dir} do
    segment_id = 1

    # Create multiple processes trying to create hint files
    tasks =
      Enum.map(1..10, fn i ->
        Task.async(fn ->
          dir = "#{test_dir}_#{i}"
          File.mkdir_p!(dir)
          entries = [{"key_#{i}", segment_id, i * 100, 50, 1_234_567_890 + i, false}]
          KVStore.Storage.Hint.create_hint(segment_id, dir, entries)
        end)
      end)

    # Wait for all tasks to complete
    results = Enum.map(tasks, &Task.await/1)

    # All should succeed
    Enum.each(results, fn result ->
      assert {:ok, _hint_path} = result
    end)

    # Verify all hint files were created correctly
    Enum.each(1..10, fn i ->
      dir = "#{test_dir}_#{i}"

      if File.exists?(dir) do
        assert {:ok, [entry]} = KVStore.Storage.Hint.read_hint(segment_id, dir)
        {key, ^segment_id, offset, 50, timestamp, false} = entry
        assert key == "key_#{i}"
        assert offset == i * 100
        assert timestamp == 1_234_567_890 + i
        File.rm_rf!(dir)
      end
    end)
  end
end
