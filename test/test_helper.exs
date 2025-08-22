# Configure ExUnit to run tests sequentially to avoid port conflicts
ExUnit.start(
  max_cases: 1,  # Run only one test case at a time
  trace: false,  # Disable tracing for cleaner output
  seed: 0        # Use consistent seed for reproducible tests
)
