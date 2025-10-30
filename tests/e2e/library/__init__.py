"""
Library integration tests for slurm-executor.

These tests validate the complete workflow of the library by:
1. Connecting to the test SLURM cluster via SSH
2. Running actual example code from the examples/ directory
3. Verifying job execution and output

Tests are organized by example and scenario:
- Happy path: Everything works as expected
- Error scenarios: Timeouts, failures, etc.
"""
