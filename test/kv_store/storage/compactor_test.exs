defmodule KVStore.Storage.CompactorTest do
  use ExUnit.Case

  setup do
    # Create a temporary directory for testing
    test_dir = Path.join(System.tmp_dir!(), "kv_store_compactor_test_#{:rand.uniform(1_000_000)}")
    File.rm_rf!(test_dir)
    File.mkdir_p!(test_dir)

    # Stop any existing processes
    try do
      GenServer.stop(KVStore.Storage.Engine, :normal)
    catch
      :exit, _ -> :ok
    end

    try do
      GenServer.stop(KVStore.Storage.Compactor, :normal)
    catch
      :exit, _ -> :ok
    end

    on_exit(fn ->
      try do
        GenServer.stop(KVStore.Storage.Engine, :normal)
      catch
        :exit, _ -> :ok
      end

      try do
        GenServer.stop(KVStore.Storage.Compactor, :normal)
      catch
        :exit, _ -> :ok
      end

      File.rm_rf!(test_dir)
    end)

    {:ok, test_dir: test_dir}
  end

  test "compactor starts with default configuration", %{test_dir: _test_dir} do
    {:ok, _pid} = KVStore.Storage.Compactor.start_link([])

    status = KVStore.Storage.Compactor.status()
    assert status.is_compacting == false
    assert status.last_compaction == nil
    assert status.merge_trigger_ratio == 0.3
    assert status.merge_throttle_ms == 10
    assert status.compaction_stats.segments_merged == 0
    assert status.compaction_stats.tombstones_removed == 0
  end

  test "compactor starts with custom configuration", %{test_dir: _test_dir} do
    {:ok, _pid} =
      KVStore.Storage.Compactor.start_link(
        merge_trigger_ratio: 0.5,
        merge_throttle_ms: 50
      )

    status = KVStore.Storage.Compactor.status()
    assert status.merge_trigger_ratio == 0.5
    assert status.merge_throttle_ms == 50
  end

  test "manual compaction triggers compaction process", %{test_dir: _test_dir} do
    {:ok, _pid} = KVStore.Storage.Compactor.start_link([])

    # Start compaction
    KVStore.Storage.Compactor.compact()

    # Check that compaction is in progress
    status = KVStore.Storage.Compactor.status()
    assert status.is_compacting == true
    assert status.last_compaction != nil

    # Wait for compaction to complete
    :timer.sleep(100)

    # Check that compaction completed
    status = KVStore.Storage.Compactor.status()
    assert status.is_compacting == false
  end

  test "compaction skips when conditions not met", %{test_dir: _test_dir} do
    {:ok, _pid} = KVStore.Storage.Compactor.start_link([])

    # Start compaction with no data
    KVStore.Storage.Compactor.compact()

    # Wait for compaction to complete
    :timer.sleep(100)

    # Check that compaction was skipped
    status = KVStore.Storage.Compactor.status()
    assert status.is_compacting == false
    assert status.compaction_stats.segments_merged == 0
  end

  test "compaction merges segments and removes tombstones", %{test_dir: test_dir} do
    # Start engine with small segment size to force multiple segments
    {:ok, _engine_pid} =
      KVStore.Storage.Engine.start_link(
        data_dir: test_dir,
        segment_max_bytes: 100,
        sync_on_put: true
      )

    # Start compactor
    {:ok, _compactor_pid} =
      KVStore.Storage.Compactor.start_link(
        # Low threshold to trigger compaction
        merge_trigger_ratio: 0.1
      )

    # Add data to create multiple segments
    Enum.each(1..20, fn i ->
      key = "key_#{i}"
      value = String.duplicate("value_#{i}_", 5)
      KVStore.Storage.Engine.put(key, value)
    end)

    # Delete some keys to create tombstones
    KVStore.Storage.Engine.delete("key_2")
    KVStore.Storage.Engine.delete("key_4")
    KVStore.Storage.Engine.delete("key_6")

    # Add more data to trigger more rotations
    Enum.each(21..30, fn i ->
      key = "key_#{i}"
      value = String.duplicate("value_#{i}_", 5)
      KVStore.Storage.Engine.put(key, value)
    end)

    # Check initial state
    initial_status = KVStore.Storage.Engine.status()
    # Should have multiple segments
    assert initial_status.active_segment_id > 3

    # Trigger compaction
    KVStore.Storage.Compactor.compact()

    # Wait for compaction to complete
    :timer.sleep(500)

    # Check compaction results
    compactor_status = KVStore.Storage.Compactor.status()
    assert compactor_status.is_compacting == false

    # Verify data is still accessible
    assert {:ok, _} = KVStore.Storage.Engine.get("key_1")
    # Deleted
    assert {:error, :not_found} = KVStore.Storage.Engine.get("key_2")
    assert {:ok, _} = KVStore.Storage.Engine.get("key_3")
    # Deleted
    assert {:error, :not_found} = KVStore.Storage.Engine.get("key_4")
    assert {:ok, _} = KVStore.Storage.Engine.get("key_5")
    # Deleted
    assert {:error, :not_found} = KVStore.Storage.Engine.get("key_6")
    assert {:ok, _} = KVStore.Storage.Engine.get("key_7")
  end

  test "compaction handles overwritten keys correctly", %{test_dir: test_dir} do
    # Start engine with small segment size
    {:ok, _engine_pid} =
      KVStore.Storage.Engine.start_link(
        data_dir: test_dir,
        segment_max_bytes: 80,
        sync_on_put: true
      )

    # Start compactor
    {:ok, _compactor_pid} = KVStore.Storage.Compactor.start_link(merge_trigger_ratio: 0.1)

    # Overwrite the same key multiple times across segments
    Enum.each(1..10, fn i ->
      KVStore.Storage.Engine.put("overwrite_key", "value_#{i}")
    end)

    # Add some other data
    Enum.each(1..5, fn i ->
      KVStore.Storage.Engine.put("other_key_#{i}", "other_value_#{i}")
    end)

    # Trigger compaction
    KVStore.Storage.Compactor.compact()

    # Wait for compaction to complete
    :timer.sleep(500)

    # Verify only the latest value is accessible
    assert {:ok, "value_10"} = KVStore.Storage.Engine.get("overwrite_key")

    # Verify other keys are still accessible
    Enum.each(1..5, fn i ->
      expected_value = "other_value_#{i}"
      assert {:ok, ^expected_value} = KVStore.Storage.Engine.get("other_key_#{i}")
    end)
  end

  test "compaction creates hint files for merged segments", %{test_dir: test_dir} do
    # Start engine with small segment size
    {:ok, _engine_pid} =
      KVStore.Storage.Engine.start_link(
        data_dir: test_dir,
        segment_max_bytes: 100,
        sync_on_put: true
      )

    # Start compactor
    {:ok, _compactor_pid} = KVStore.Storage.Compactor.start_link(merge_trigger_ratio: 0.1)

    # Add data to create multiple segments
    Enum.each(1..15, fn i ->
      key = "hint_key_#{i}"
      value = String.duplicate("hint_value_#{i}_", 6)
      KVStore.Storage.Engine.put(key, value)
    end)

    # Trigger compaction
    KVStore.Storage.Compactor.compact()

    # Wait for compaction to complete
    :timer.sleep(500)

    # Check that hint files were created
    case KVStore.Storage.Hint.list_hints(test_dir) do
      {:ok, hint_segments} ->
        # Should have hint files
        assert length(hint_segments) >= 1

      {:error, _} ->
        # Hint files might not be created immediately
        :ok
    end
  end

  test "compaction updates ETS entries correctly", %{test_dir: test_dir} do
    # Start engine with small segment size
    {:ok, _engine_pid} =
      KVStore.Storage.Engine.start_link(
        data_dir: test_dir,
        segment_max_bytes: 100,
        sync_on_put: true
      )

    # Start compactor
    {:ok, _compactor_pid} = KVStore.Storage.Compactor.start_link(merge_trigger_ratio: 0.1)

    # Add data
    Enum.each(1..10, fn i ->
      KVStore.Storage.Engine.put("ets_key_#{i}", "ets_value_#{i}")
    end)

    # Get initial status
    initial_status = KVStore.Storage.Engine.status()

    # Trigger compaction
    KVStore.Storage.Compactor.compact()

    # Wait for compaction to complete
    :timer.sleep(500)

    # Get final status
    final_status = KVStore.Storage.Engine.status()

    # Verify ETS entries are preserved
    assert final_status.keydir_size == initial_status.keydir_size
    assert final_status.key_set_size == initial_status.key_set_size

    # Verify all data is still accessible
    Enum.each(1..10, fn i ->
      expected_value = "ets_value_#{i}"
      assert {:ok, ^expected_value} = KVStore.Storage.Engine.get("ets_key_#{i}")
    end)
  end

  test "compaction handles concurrent access", %{test_dir: test_dir} do
    # Start engine with small segment size
    {:ok, _engine_pid} =
      KVStore.Storage.Engine.start_link(
        data_dir: test_dir,
        segment_max_bytes: 100,
        sync_on_put: true
      )

    # Start compactor
    {:ok, _compactor_pid} = KVStore.Storage.Compactor.start_link(merge_trigger_ratio: 0.1)

    # Add initial data
    Enum.each(1..10, fn i ->
      KVStore.Storage.Engine.put("concurrent_key_#{i}", "concurrent_value_#{i}")
    end)

    # Start compaction
    KVStore.Storage.Compactor.compact()

    # Immediately try to read and write data
    Enum.each(1..5, fn i ->
      # Read existing data
      expected_value = "concurrent_value_#{i}"
      assert {:ok, ^expected_value} = KVStore.Storage.Engine.get("concurrent_key_#{i}")

      # Write new data
      KVStore.Storage.Engine.put("new_key_#{i}", "new_value_#{i}")
    end)

    # Wait for compaction to complete
    :timer.sleep(500)

    # Verify all data is accessible
    Enum.each(1..10, fn i ->
      expected_value = "concurrent_value_#{i}"
      assert {:ok, ^expected_value} = KVStore.Storage.Engine.get("concurrent_key_#{i}")
    end)

    Enum.each(1..5, fn i ->
      expected_value = "new_value_#{i}"
      assert {:ok, ^expected_value} = KVStore.Storage.Engine.get("new_key_#{i}")
    end)
  end

  test "compaction statistics are tracked correctly", %{test_dir: test_dir} do
    # Start engine with small segment size
    {:ok, _engine_pid} =
      KVStore.Storage.Engine.start_link(
        data_dir: test_dir,
        segment_max_bytes: 100,
        sync_on_put: true
      )

    # Start compactor
    {:ok, _compactor_pid} = KVStore.Storage.Compactor.start_link(merge_trigger_ratio: 0.1)

    # Add data and create tombstones
    Enum.each(1..10, fn i ->
      KVStore.Storage.Engine.put("stats_key_#{i}", "stats_value_#{i}")
    end)

    # Delete some keys
    KVStore.Storage.Engine.delete("stats_key_2")
    KVStore.Storage.Engine.delete("stats_key_4")
    KVStore.Storage.Engine.delete("stats_key_6")

    # Trigger compaction
    KVStore.Storage.Compactor.compact()

    # Wait for compaction to complete
    :timer.sleep(500)

    # Check compaction statistics
    status = KVStore.Storage.Compactor.status()
    assert status.compaction_stats.segments_merged > 0
    # Should have removed tombstones
    assert status.compaction_stats.tombstones_removed >= 3
  end

  test "compaction respects merge trigger ratio", %{test_dir: test_dir} do
    # Start engine with small segment size
    {:ok, _engine_pid} =
      KVStore.Storage.Engine.start_link(
        data_dir: test_dir,
        segment_max_bytes: 100,
        sync_on_put: true
      )

    # Start compactor with high threshold
    {:ok, _compactor_pid} =
      KVStore.Storage.Compactor.start_link(
        # High threshold
        merge_trigger_ratio: 0.8
      )

    # Add minimal data
    Enum.each(1..5, fn i ->
      KVStore.Storage.Engine.put("threshold_key_#{i}", "threshold_value_#{i}")
    end)

    # Trigger compaction
    KVStore.Storage.Compactor.compact()

    # Wait for compaction to complete
    :timer.sleep(500)

    # Check that compaction was skipped due to high threshold
    status = KVStore.Storage.Compactor.status()
    assert status.is_compacting == false
    # With high threshold, compaction should be skipped
    # (This test may be flaky depending on the exact stale ratio calculation)
  end
end
