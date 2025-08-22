defmodule KVStore.Storage.IsolatedTestExample do
  use ExUnit.Case

  setup do
    # Use the test helper to set up isolated storage environment
    {test_dir, cleanup_fn} = KVStore.TestHelper.setup_isolated_storage(segment_max_bytes: 100)

    on_exit(cleanup_fn)

    {:ok, test_dir: test_dir}
  end

  test "basic storage operations work with isolated environment", %{test_dir: test_dir} do
    # Test that all storage processes are running
    assert Process.whereis(KVStore.Storage.Engine) != nil
    assert Process.whereis(KVStore.Storage.Cache) != nil
    assert Process.whereis(KVStore.Storage.FileCache) != nil
    assert Process.whereis(KVStore.Storage.Compactor) != nil

    # Test basic operations
    assert {:ok, _offset} = KVStore.Storage.Engine.put("test_key", "test_value")
    assert {:ok, "test_value"} = KVStore.Storage.Engine.get("test_key")

    # Test that the engine status shows our test directory
    status = KVStore.Storage.Engine.status()
    assert String.contains?(status.data_dir, "kv_store_test_")
    assert status.segment_max_bytes == 100
  end

  test "segment rotation works with isolated environment", %{test_dir: test_dir} do
    # Generate data that will trigger segment rotation
    large_value = String.duplicate("x", 50)

    # Put enough data to trigger rotation
    assert {:ok, _} = KVStore.Storage.Engine.put("key1", large_value)
    assert {:ok, _} = KVStore.Storage.Engine.put("key2", large_value)
    assert {:ok, _} = KVStore.Storage.Engine.put("key3", large_value)

    # Check that we have rotated segments
    status = KVStore.Storage.Engine.status()
    assert status.active_segment_id >= 2

    # Verify all data is still accessible
    assert {:ok, ^large_value} = KVStore.Storage.Engine.get("key1")
    assert {:ok, ^large_value} = KVStore.Storage.Engine.get("key2")
    assert {:ok, ^large_value} = KVStore.Storage.Engine.get("key3")
  end

  test "cache integration works properly", %{test_dir: test_dir} do
    # Put some data
    assert {:ok, _} = KVStore.Storage.Engine.put("cached_key", "cached_value")

    # Get it to populate cache
    assert {:ok, "cached_value"} = KVStore.Storage.Engine.get("cached_key")

    # Check cache stats
    cache_stats = KVStore.Storage.Cache.stats()
    assert is_map(cache_stats)
    assert cache_stats.current_entries >= 0
  end
end
