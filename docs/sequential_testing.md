# Sequential Testing Approach

## Overview

This document explains the sequential testing approach implemented for the KV Store project to avoid port conflicts and setup issues.

## Problem

When running tests in parallel, the following issues occur:

1. **Port Conflicts**: Multiple tests try to start HTTP/Binary servers on the same ports
2. **Setup Conflicts**: Tests interfere with each other's environment setup
3. **Resource Contention**: Shared resources cause unpredictable test failures
4. **Debugging Difficulty**: Hard to isolate which test is causing issues

## Solution: Sequential Testing

### Configuration

The test configuration has been updated to run tests sequentially:

```elixir
# test/test_helper.exs
ExUnit.start(
  max_cases: 1,  # Run only one test case at a time
  trace: false,  # Disable tracing for cleaner output
  seed: 0        # Use consistent seed for reproducible tests
)
```

### Fresh Environment Setup

Each test file gets a fresh environment using the `KVStore.FreshEnvironment` module:

```elixir
# test/support/fresh_environment.ex
defmodule KVStore.FreshEnvironment do
  def setup_fresh_environment do
    # Create unique test directory
    test_dir = Path.join(System.tmp_dir!(), "kv_store_test_#{:rand.uniform(1_000_000)}")
    
    # Set isolated environment variables
    env_vars = %{
      "KV_DATA_DIR" => test_dir,
      "KV_PORT" => "#{5000 + :rand.uniform(1000)}",
      "KV_BINARY_PORT" => "#{6000 + :rand.uniform(1000)}",
      # ... other variables
    }
    
    # Clean application state
    Application.stop(:kv_store)
    Application.unload(:kv_store)
    
    {:ok, test_dir: test_dir, cleanup: cleanup_fn, env_vars: env_vars}
  end
end
```

### Test File Setup

Test files use the fresh environment setup:

```elixir
defmodule KVStore.Storage.WALTest do
  use ExUnit.Case

  setup do
    # Set up fresh environment for each test
    {:ok, [test_dir: test_dir, cleanup: cleanup_fn, env_vars: _env_vars]} = 
      KVStore.FreshEnvironment.setup_fresh_environment()

    on_exit(fn ->
      KVStore.FreshEnvironment.cleanup_environment(cleanup_fn)
    end)

    {:ok, test_dir: test_dir}
  end
  
  # ... tests
end
```

## Benefits

### ✅ No Port Conflicts
- Each test gets unique, randomly assigned ports
- No interference between HTTP/Binary servers

### ✅ Fresh Environment
- Each test starts with a clean slate
- No leftover state from previous tests
- Isolated temporary directories

### ✅ Easier Debugging
- Tests run one at a time
- Clear error attribution
- Reproducible test failures

### ✅ More Reliable
- Consistent test results
- No race conditions
- Predictable test behavior

## Usage

### Running Tests Sequentially

```bash
# Run all tests sequentially
mix test

# Run specific test file
mix test test/kv_store/storage/wal_test.exs

# Run with trace for debugging
mix test --trace
```

### Using the Sequential Test Script

```bash
# Run storage tests sequentially
elixir scripts/run_storage_tests_sequentially.exs

# Run all tests sequentially
elixir scripts/run_tests_sequentially.exs
```

## Test Categories

### ✅ Storage Tests (Work Well)
- `test/kv_store/storage/wal_test.exs` - ✅ All 13 tests pass
- `test/kv_store/storage/segment_rotation_test.exs` - Needs fixes
- `test/kv_store/storage/performance_test.exs` - Needs fixes

### ⚠️ Server Tests (Need Additional Setup)
- `test/kv_store/server_test.exs` - Need port isolation
- `test/kv_store/binary_server_test.exs` - Need port isolation
- `test/kv_store/client_test.exs` - Need server setup

## Implementation Status

### Completed
- ✅ Sequential test configuration
- ✅ Fresh environment setup module
- ✅ WAL tests working with fresh environment
- ✅ Sequential test runner script

### In Progress
- 🔄 Fixing remaining storage test failures
- 🔄 Improving server test port isolation
- 🔄 Enhancing client test setup

## Best Practices

1. **Always use fresh environment setup** for new test files
2. **Clean up resources** in `on_exit` callbacks
3. **Use unique ports** for server tests
4. **Isolate test data** in temporary directories
5. **Run tests sequentially** during development

## Troubleshooting

### Common Issues

1. **Port still in use**: Increase port range or add delay between tests
2. **File system conflicts**: Ensure proper cleanup in `on_exit`
3. **Application state**: Always stop/unload application before setup

### Debugging Tips

1. Use `--trace` flag for detailed output
2. Check temporary directory contents
3. Verify environment variables are set correctly
4. Monitor port usage with `lsof -i :PORT`

## Future Improvements

1. **Parallel-safe server tests**: Implement proper port management
2. **Test isolation framework**: Create reusable test isolation patterns
3. **Performance optimization**: Balance isolation with test speed
4. **CI/CD integration**: Ensure sequential testing works in CI environments
