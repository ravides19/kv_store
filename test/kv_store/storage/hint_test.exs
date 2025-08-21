defmodule KVStore.Storage.HintTest do
  use ExUnit.Case

  setup do
    # Create a temporary directory for testing
    test_dir = Path.join(System.tmp_dir!(), "kv_store_hint_test_#{:rand.uniform(1_000_000)}")
    File.mkdir_p!(test_dir)

    on_exit(fn ->
      File.rm_rf!(test_dir)
    end)

    {:ok, test_dir: test_dir}
  end

  test "creates and reads hint file correctly", %{test_dir: test_dir} do
    segment_id = 1

    keydir_entries = [
      {"key1", segment_id, 0, 50, 1_234_567_890, false},
      {"key2", segment_id, 50, 60, 1_234_567_891, false},
      # tombstone
      {"key3", segment_id, 110, 40, 1_234_567_892, true}
    ]

    # Create hint file
    assert {:ok, hint_path} =
             KVStore.Storage.Hint.create_hint(segment_id, test_dir, keydir_entries)

    assert File.exists?(hint_path)

    # Read hint file
    assert {:ok, read_entries} = KVStore.Storage.Hint.read_hint(segment_id, test_dir)
    assert length(read_entries) == 3

    # Verify entries match
    Enum.zip(keydir_entries, read_entries)
    |> Enum.each(fn {original, read} ->
      assert original == read
    end)
  end

  test "handles empty keydir entries", %{test_dir: test_dir} do
    segment_id = 2

    # Create hint file with no entries
    assert {:ok, hint_path} = KVStore.Storage.Hint.create_hint(segment_id, test_dir, [])
    assert File.exists?(hint_path)

    # Read hint file
    assert {:ok, read_entries} = KVStore.Storage.Hint.read_hint(segment_id, test_dir)
    assert read_entries == []
  end

  test "returns empty list for non-existent hint file", %{test_dir: test_dir} do
    segment_id = 999

    # Try to read non-existent hint file
    assert {:ok, entries} = KVStore.Storage.Hint.read_hint(segment_id, test_dir)
    assert entries == []
  end

  test "lists hint files correctly", %{test_dir: test_dir} do
    # Create some hint files
    KVStore.Storage.Hint.create_hint(1, test_dir, [{"key1", 1, 0, 50, 1_234_567_890, false}])
    KVStore.Storage.Hint.create_hint(3, test_dir, [{"key3", 3, 0, 50, 1_234_567_890, false}])
    KVStore.Storage.Hint.create_hint(2, test_dir, [{"key2", 2, 0, 50, 1_234_567_890, false}])

    # List hint files
    assert {:ok, hint_segments} = KVStore.Storage.Hint.list_hints(test_dir)
    # Should be sorted
    assert hint_segments == [1, 2, 3]
  end

  test "deletes hint file correctly", %{test_dir: test_dir} do
    segment_id = 1
    keydir_entries = [{"key1", segment_id, 0, 50, 1_234_567_890, false}]

    # Create hint file
    KVStore.Storage.Hint.create_hint(segment_id, test_dir, keydir_entries)
    assert KVStore.Storage.Hint.hint_exists?(segment_id, test_dir)

    # Delete hint file
    assert :ok = KVStore.Storage.Hint.delete_hint(segment_id, test_dir)
    assert not KVStore.Storage.Hint.hint_exists?(segment_id, test_dir)
  end

  test "handles deletion of non-existent hint file", %{test_dir: test_dir} do
    segment_id = 999

    # Try to delete non-existent hint file
    assert :ok = KVStore.Storage.Hint.delete_hint(segment_id, test_dir)
  end

  test "handles binary and term keys correctly", %{test_dir: test_dir} do
    segment_id = 1

    keydir_entries = [
      {"string_key", segment_id, 0, 50, 1_234_567_890, false},
      {["list", "key"], segment_id, 50, 60, 1_234_567_891, false},
      {{:tuple, "key"}, segment_id, 110, 40, 1_234_567_892, false}
    ]

    # Create hint file
    assert {:ok, _hint_path} =
             KVStore.Storage.Hint.create_hint(segment_id, test_dir, keydir_entries)

    # Read hint file
    assert {:ok, read_entries} = KVStore.Storage.Hint.read_hint(segment_id, test_dir)
    assert length(read_entries) == 3

    # Verify entries match
    Enum.zip(keydir_entries, read_entries)
    |> Enum.each(fn {original, read} ->
      assert original == read
    end)
  end
end
