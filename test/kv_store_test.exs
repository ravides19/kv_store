defmodule KVStoreTest do
  use ExUnit.Case
  doctest KVStore

  setup do
    # Start the application for each test
    KVStore.start()
    :ok
  end

  test "application starts successfully" do
    # Verify that the application is running
    assert Process.whereis(KVStore.Supervisor) != nil
    assert Process.whereis(KVStore.Storage.Supervisor) != nil
    assert Process.whereis(KVStore.Storage.Engine) != nil
    assert Process.whereis(KVStore.Storage.FileCache) != nil
    assert Process.whereis(KVStore.Storage.Compactor) != nil
  end

  test "storage engine status returns expected structure" do
    status = KVStore.Storage.Engine.status()

    assert is_map(status)
    assert Map.has_key?(status, :data_dir)
    assert Map.has_key?(status, :segment_max_bytes)
    assert Map.has_key?(status, :sync_on_put)
    assert Map.has_key?(status, :active_segment_id)
    assert Map.has_key?(status, :active_offset)
    assert Map.has_key?(status, :keydir_size)
    assert Map.has_key?(status, :key_set_size)

    assert status.data_dir == "data"
    assert status.segment_max_bytes == 100 * 1024 * 1024
    assert status.sync_on_put == false
    assert status.active_segment_id == 1
    assert status.active_offset == 0
    assert status.keydir_size == 0
    assert status.key_set_size == 0
  end

  test "compactor status returns expected structure" do
    status = KVStore.Storage.Compactor.status()

    assert is_map(status)
    assert Map.has_key?(status, :is_compacting)
    assert Map.has_key?(status, :last_compaction)
    assert Map.has_key?(status, :merge_trigger_ratio)
    assert Map.has_key?(status, :merge_throttle_ms)

    assert status.is_compacting == false
    assert status.merge_trigger_ratio == 0.3
    assert status.merge_throttle_ms == 10
  end

  test "main KVStore API functions return not implemented yet" do
    # These operations are not yet implemented in Phase 0
    assert KVStore.put("key", "value") == {:ok, :not_implemented_yet}
    assert KVStore.get("key") == {:ok, :not_implemented_yet}
    assert KVStore.delete("key") == {:ok, :not_implemented_yet}
    assert KVStore.range("start", "end") == {:ok, :not_implemented_yet}
    assert KVStore.batch_put([{"key", "value"}]) == {:ok, :not_implemented_yet}
  end

  test "configuration module provides expected configs" do
    storage_config = KVStore.Config.storage_config()
    file_cache_config = KVStore.Config.file_cache_config()
    compactor_config = KVStore.Config.compactor_config()
    network_config = KVStore.Config.network_config()

    assert is_list(storage_config)
    assert is_list(file_cache_config)
    assert is_list(compactor_config)
    assert is_list(network_config)

    # Check storage config
    assert Keyword.get(storage_config, :data_dir) == "data"
    assert Keyword.get(storage_config, :segment_max_bytes) == 100 * 1024 * 1024
    assert Keyword.get(storage_config, :sync_on_put) == false

    # Check file cache config
    assert Keyword.get(file_cache_config, :max_files) == 10

    # Check compactor config
    assert Keyword.get(compactor_config, :merge_trigger_ratio) == 0.3
    assert Keyword.get(compactor_config, :merge_throttle_ms) == 10

    # Check network config
    assert Keyword.get(network_config, :port) == 8080
    assert Keyword.get(network_config, :host) == "127.0.0.1"
  end
end
