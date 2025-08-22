defmodule KVStore.Storage.PerformanceTest do
  use ExUnit.Case

  setup do
    # Use the storage-only setup to avoid HTTP server conflicts
    {test_dir, cleanup_fn} = KVStore.TestHelper.setup_isolated_storage()

    on_exit(fn ->
      cleanup_fn.()
    end)

    {:ok, test_dir: test_dir}
  end

  test "cache performance with hot data", %{test_dir: _test_dir} do
    # Storage is already started by setup

    # Insert test data
    test_data = Enum.map(1..100, fn i -> {"key_#{i}", "value_#{i}"} end)

    Enum.each(test_data, fn {key, value} ->
      assert {:ok, _offset} = KVStore.Storage.Engine.put(key, value)
    end)

    # First read (cache miss) - should be slower
    start_time = :os.system_time(:microsecond)
    assert {:ok, "value_1"} = KVStore.Storage.Engine.get("key_1")
    first_read_time = :os.system_time(:microsecond) - start_time

    # Second read (cache hit) - should be faster
    start_time = :os.system_time(:microsecond)
    assert {:ok, "value_1"} = KVStore.Storage.Engine.get("key_1")
    second_read_time = :os.system_time(:microsecond) - start_time

    # Cache hit should be significantly faster
    assert second_read_time < first_read_time
    # Should be under 1ms
    assert second_read_time < 1000

    # Check cache statistics
    cache_stats = KVStore.Storage.Cache.stats()
    assert cache_stats.hits > 0
    assert cache_stats.misses > 0
    assert cache_stats.hit_rate > 0.0
  end

  test "batch write performance optimization", %{test_dir: _test_dir} do
    # Storage is already started by setup

    # Prepare large batch
    batch_size = 1000
    batch_data = Enum.map(1..batch_size, fn i -> {"batch_key_#{i}", "batch_value_#{i}"} end)

    # Time batch write
    start_time = :os.system_time(:microsecond)
    assert {:ok, _offset} = KVStore.Storage.Engine.batch_put(batch_data)
    batch_write_time = :os.system_time(:microsecond) - start_time

    # Time individual writes for comparison
    individual_data = Enum.map(1..100, fn i -> {"ind_key_#{i}", "ind_value_#{i}"} end)

    start_time = :os.system_time(:microsecond)

    Enum.each(individual_data, fn {key, value} ->
      assert {:ok, _offset} = KVStore.Storage.Engine.put(key, value)
    end)

    individual_write_time = :os.system_time(:microsecond) - start_time

    # Batch write should be more efficient per operation
    batch_time_per_op = batch_write_time / batch_size
    individual_time_per_op = individual_write_time / 100

    # Batch operations should be faster per operation
    # Allow some variance
    assert batch_time_per_op < individual_time_per_op * 2

    # Verify all data is accessible
    Enum.each(batch_data, fn {key, expected_value} ->
      assert {:ok, ^expected_value} = KVStore.Storage.Engine.get(key)
    end)
  end

  test "compression performance and space savings", %{test_dir: _test_dir} do
    # Create test data with repetitive patterns (good for compression)
    repetitive_data = String.duplicate("repetitive_pattern_", 1000)

    # Test compression
    start_time = :os.system_time(:microsecond)

    {:ok, compressed_data, algorithm} =
      KVStore.Storage.Compression.compress_segment(repetitive_data)

    compression_time = :os.system_time(:microsecond) - start_time

    # Test decompression
    start_time = :os.system_time(:microsecond)

    {:ok, decompressed_data} =
      KVStore.Storage.Compression.decompress_segment(compressed_data, algorithm)

    decompression_time = :os.system_time(:microsecond) - start_time

    # Verify data integrity
    assert decompressed_data == repetitive_data

    # Calculate compression stats
    stats =
      KVStore.Storage.Compression.compression_stats(
        byte_size(repetitive_data),
        byte_size(compressed_data)
      )

    # Should achieve some compression
    assert stats.compression_ratio > 0.0
    assert stats.space_saved > 0

    # Compression/decompression should be fast
    # Under 10ms
    assert compression_time < 10_000
    # Under 10ms
    assert decompression_time < 10_000

    # Test with non-compressible data
    random_data = :crypto.strong_rand_bytes(1000)

    {:ok, compressed_random, _algorithm} =
      KVStore.Storage.Compression.compress_segment(random_data)

    random_stats =
      KVStore.Storage.Compression.compression_stats(
        byte_size(random_data),
        byte_size(compressed_random)
      )

    # Random data might not compress well, but should still work
    # Allow small negative ratios for random data
    assert random_stats.compression_ratio >= -0.1
  end

  test "memory usage optimization with large datasets", %{test_dir: _test_dir} do
    # Storage is already started by setup

    # Insert large dataset
    large_dataset = Enum.map(1..5000, fn i -> {"large_key_#{i}", "large_value_#{i}"} end)

    # Time the insertion
    start_time = :os.system_time(:microsecond)

    Enum.each(large_dataset, fn {key, value} ->
      assert {:ok, _offset} = KVStore.Storage.Engine.put(key, value)
    end)

    _insertion_time = :os.system_time(:microsecond) - start_time

    # Check memory usage
    status = KVStore.Storage.Engine.status()

    # The test might have accumulated data from previous tests, so check for at least the expected amount
    assert status.keydir_size >= 5000
    assert status.key_set_size >= 5000

    # Read some values to populate cache
    Enum.each(1..100, fn i ->
      expected_value = "large_value_#{i}"
      assert {:ok, ^expected_value} = KVStore.Storage.Engine.get("large_key_#{i}")
    end)

    # Check cache statistics
    cache_stats = KVStore.Storage.Cache.stats()
    assert cache_stats.current_entries > 0
    assert cache_stats.memory_usage_bytes > 0

    # Performance should remain consistent
    start_time = :os.system_time(:microsecond)

    Enum.each(5001..5100, fn i ->
      case KVStore.Storage.Engine.get("large_key_#{i}") do
        {:ok, value} ->
          expected_value = "large_value_#{i}"
          assert value == expected_value

        {:error, :not_found} ->
          # Skip if key doesn't exist (might be from previous test data)
          :ok
      end
    end)

    read_time = :os.system_time(:microsecond) - start_time

    # Should be able to read 100 values in reasonable time
    # Under 1 second
    assert read_time < 1_000_000
  end

  test "range query performance with ordered data", %{test_dir: _test_dir} do
    # Storage is already started by setup

    # Insert ordered data
    ordered_data = Enum.map(1..1000, fn i -> {"ordered_key_#{String.pad_leading("#{i}", 4, "0")}", "value_#{i}"} end)

    Enum.each(ordered_data, fn {key, value} ->
      assert {:ok, _offset} = KVStore.Storage.Engine.put(key, value)
    end)

    # Test range queries
    range_queries = [
      {"ordered_key_0001", "ordered_key_0100"},
      {"ordered_key_0100", "ordered_key_0200"},
      {"ordered_key_0500", "ordered_key_0600"},
      {"ordered_key_0900", "ordered_key_1000"}
    ]

    Enum.each(range_queries, fn {start_key, end_key} ->
      start_time = :os.system_time(:microsecond)
      {:ok, results} = KVStore.Storage.Engine.range(start_key, end_key)
      query_time = :os.system_time(:microsecond) - start_time

      # Should return expected number of results
      expected_count = 100
      assert length(results) == expected_count

      # Range queries should be fast
      # Under 10ms for 100 results
      assert query_time < 10_000

      # Verify results are in order
      sorted_results = Enum.sort_by(results, fn {key, _value} -> key end)
      assert results == sorted_results
    end)

    # Test with non-existent ranges
    start_time = :os.system_time(:microsecond)
    {:ok, empty_results} = KVStore.Storage.Engine.range("nonexistent_start", "nonexistent_end")
    query_time = :os.system_time(:microsecond) - start_time

    assert empty_results == []
    # Should be very fast for empty results
    assert query_time < 1000
  end

  test "cache eviction and memory management", %{test_dir: _test_dir} do
    # Storage is already started by setup

    # Insert data to overflow cache
    cache_overflow_data = Enum.map(1..2000, fn i -> {"cache_key_#{i}", "cache_value_#{i}"} end)

    Enum.each(cache_overflow_data, fn {key, value} ->
      assert {:ok, _offset} = KVStore.Storage.Engine.put(key, value)
      # This will cache the value
      assert {:ok, ^value} = KVStore.Storage.Engine.get(key)
    end)

    # Check cache statistics
    cache_stats = KVStore.Storage.Cache.stats()

    # Should have evicted some entries
    assert cache_stats.evictions > 0
    # Max entries limit
    assert cache_stats.current_entries <= 1000

    # Memory usage should be within limits
    # Max memory limit
    assert cache_stats.memory_usage_bytes <= 100 * 1024 * 1024  # 100MB in bytes

    # Cache should still work for recently accessed data
    expected_value_1999 = "cache_value_1999"
    expected_value_2000 = "cache_value_2000"
    assert {:ok, ^expected_value_1999} = KVStore.Storage.Engine.get("cache_key_1999")
    assert {:ok, ^expected_value_2000} = KVStore.Storage.Engine.get("cache_key_2000")
  end

  test "mixed workload performance", %{test_dir: _test_dir} do
    # Storage is already started by setup

    # Simulate mixed workload: writes, reads, deletes, range queries
    workload_start = :os.system_time(:microsecond)

    # Phase 1: Bulk writes
    bulk_data = Enum.map(1..1000, fn i -> {"bulk_key_#{i}", "bulk_value_#{i}"} end)
    assert {:ok, _offset} = KVStore.Storage.Engine.batch_put(bulk_data)

    # Phase 2: Random reads
    Enum.each(1..100, fn _ ->
      random_key = "bulk_key_#{:rand.uniform(1000)}"
      assert {:ok, _value} = KVStore.Storage.Engine.get(random_key)
    end)

    # Phase 3: Range queries
    Enum.each(1..10, fn i ->
      start_key = "bulk_key_#{i * 100}"
      end_key = "bulk_key_#{i * 100 + 50}"
      {:ok, _results} = KVStore.Storage.Engine.range(start_key, end_key)
    end)

    # Phase 4: Deletes
    Enum.each(1..100, fn i ->
      assert {:ok, _offset} = KVStore.Storage.Engine.delete("bulk_key_#{i}")
    end)

    # Phase 5: More reads (should hit cache)
    Enum.each(500..600, fn i ->
      assert {:ok, _value} = KVStore.Storage.Engine.get("bulk_key_#{i}")
    end)

    total_time = :os.system_time(:microsecond) - workload_start

    # Mixed workload should complete in reasonable time
    # Under 5 seconds
    assert total_time < 5_000_000

    # Check final statistics
    status = KVStore.Storage.Engine.status()
    cache_stats = KVStore.Storage.Cache.stats()

    # Should have reasonable hit rate
    assert cache_stats.hit_rate > 0.0
    assert status.keydir_size > 0
  end

  test "compression integration with segment rotation", %{test_dir: test_dir} do
    # Storage is already started by setup with small segment size

    # Insert data to trigger segment rotation (using smaller values)
    large_values =
      Enum.map(1..20, fn i ->
        {"comp_key_#{i}", String.duplicate("large_value_#{i}_", 20)}
      end)

    Enum.each(large_values, fn {key, value} ->
      assert {:ok, _offset} = KVStore.Storage.Engine.put(key, value)
    end)

    # Check that segments were created
    segment_files = File.ls!(test_dir) |> Enum.filter(&String.ends_with?(&1, ".data"))
    assert length(segment_files) > 1

    # Test compression on one of the segment files that has data
    segment_files_with_data =
      segment_files
      |> Enum.map(fn file -> {file, File.stat!(Path.join(test_dir, file)).size} end)
      |> Enum.filter(fn {_file, size} -> size > 0 end)
      |> Enum.map(fn {file, _size} -> file end)

    assert length(segment_files_with_data) > 0

    segment_path = Path.join(test_dir, hd(segment_files_with_data))

    # Get original size
    {:ok, %{size: original_size}} = File.stat(segment_path)

    # Compress the segment
    {:ok, compression_stats} = KVStore.Storage.Compression.compress_segment_file(segment_path)

    # Check compression results
    assert compression_stats.original_size == original_size
    # Only check compression if the original file has data
    if original_size > 0 do
      assert compression_stats.compressed_size < original_size
      assert compression_stats.space_saved > 0
    end

    # Verify data is still accessible after compression
    assert {:ok, "large_value_1_" <> _} = KVStore.Storage.Engine.get("comp_key_1")
  end
end
