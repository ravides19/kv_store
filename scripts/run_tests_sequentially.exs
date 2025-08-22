#!/usr/bin/env elixir

# Script to run tests sequentially with fresh environments
# This avoids port conflicts and setup issues

IO.puts("🧪 Running KV Store Tests Sequentially")
IO.puts("=" |> String.duplicate(50))

# List of test files to run
test_files = [
  "test/kv_store/storage/wal_test.exs",
  "test/kv_store/storage/segment_rotation_test.exs",
  "test/kv_store/storage/performance_test.exs",
  "test/kv_store/server_test.exs",
  "test/kv_store/binary_server_test.exs",
  "test/kv_store/client_test.exs"
]

# Run each test file individually
results = Enum.map(test_files, fn test_file ->
  IO.puts("\n📁 Running: #{test_file}")
  IO.puts("-" |> String.duplicate(40))

  # Run the test file
  {output, exit_code} = System.cmd("mix", ["test", test_file, "--no-compile"],
    stderr_to_stdout: true,
    env: [
      {"MIX_ENV", "test"},
      {"EXUNIT_MAX_CASES", "1"}  # Ensure sequential execution
    ]
  )

  # Parse results
  case exit_code do
    0 ->
      IO.puts("✅ PASSED")
      {:passed, test_file}
    _ ->
      IO.puts("❌ FAILED")
      IO.puts("Output:")
      IO.puts(output)
      {:failed, test_file}
  end
end)

# Summary
IO.puts("\n" |> String.duplicate(50))
IO.puts("📊 TEST SUMMARY")
IO.puts("=" |> String.duplicate(50))

passed = Enum.count(results, fn {status, _} -> status == :passed end)
failed = Enum.count(results, fn {status, _} -> status == :failed end)

IO.puts("✅ Passed: #{passed}")
IO.puts("❌ Failed: #{failed}")
IO.puts("📁 Total: #{length(results)}")

if failed > 0 do
  IO.puts("\n❌ Failed tests:")
  Enum.each(results, fn {status, test_file} ->
    if status == :failed do
      IO.puts("  - #{test_file}")
    end
  end)
end

IO.puts("\n🎯 Benefits of sequential testing:")
IO.puts("  • No port conflicts")
IO.puts("  • Fresh environment for each test file")
IO.puts("  • Isolated test execution")
IO.puts("  • Easier debugging")
IO.puts("  • More reliable test results")

exit(if failed > 0, do: 1, else: 0)
