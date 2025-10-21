#!/usr/bin/env python3
"""
Test runner for SSH mock examples.

This script demonstrates how to use the SSH mock without pytest,
showing that the mock works independently of any test framework.
"""

import sys
from pathlib import Path

# Add src to path so we can import slurm_executor
sys.path.insert(0, str(Path(__file__).parent / "src"))

from slurm_executor.tests.expectations import (
    expect_rsync_operations,
    expect_slurm_job_monitoring,
    expect_slurm_job_submission,
)
from slurm_executor.tests.ssh_mock import (
    MockSSHConnection,
)


def test_basic_functionality():
    """Test basic mock functionality."""
    print("Testing basic SSH mock functionality...")

    mock_conn = MockSSHConnection()
    mock_conn.expect_command("echo hello", stdout="hello\n")

    result = mock_conn.run("echo hello")
    assert result.stdout == "hello\n"
    assert result.return_code == 0

    mock_conn.assert_command_executed("echo hello")
    print("✓ Basic functionality works")


def test_regex_matching():
    """Test regex command matching."""
    print("Testing regex command matching...")

    mock_conn = MockSSHConnection()
    mock_conn.expect_command(
        r"ls -la /.*", stdout="file1.txt\nfile2.txt\n", is_regex=True
    )

    result1 = mock_conn.run("ls -la /home")
    result2 = mock_conn.run("ls -la /tmp")

    assert result1.stdout == "file1.txt\nfile2.txt\n"
    assert result2.stdout == "file1.txt\nfile2.txt\n"

    print("✓ Regex matching works")


def test_slurm_helpers():
    """Test SLURM helper functions."""
    print("Testing SLURM helper functions...")

    mock_conn = MockSSHConnection()

    # Test job submission helper
    expect_slurm_job_submission(mock_conn, "42", "/test/workspace")

    # Test rsync helper
    expect_rsync_operations(mock_conn, "/test/workspace")

    # Test job monitoring helper
    expect_slurm_job_monitoring(
        mock_conn, "42", "COMPLETED", ["PENDING"], "/test/workspace/job.out"
    )

    # Simulate some operations
    mock_conn.run("mkdir -p /test/workspace")
    result = mock_conn.run(
        "cd /test/workspace && "
        "sbatch --parsable --output=/test/workspace/job.out script.sh"
    )
    assert "42" in result.stdout

    print("✓ SLURM helpers work")


def test_error_conditions():
    """Test error simulation."""
    print("Testing error simulation...")

    mock_conn = MockSSHConnection()
    mock_conn.expect_command(
        "failing_command", stderr="Permission denied\n", exit_code=1
    )

    result = mock_conn.run("failing_command")
    assert result.return_code == 1
    assert result.failed is True
    assert "Permission denied" in result.stderr

    print("✓ Error simulation works")


def test_strict_mode():
    """Test strict mode."""
    print("Testing strict mode...")

    mock_conn = MockSSHConnection().set_strict_mode(True)
    mock_conn.expect_command("allowed", stdout="ok")

    # This should work
    mock_conn.run("allowed")

    # This should fail
    try:
        mock_conn.run("not_allowed")
        raise AssertionError("Expected an error for unexpected command")
    except AssertionError as e:
        assert "Unexpected command" in str(e)

    print("✓ Strict mode works")


def main():
    """Run all tests."""
    print("Running SSH Mock Tests")
    print("=" * 40)

    tests = [
        test_basic_functionality,
        test_regex_matching,
        test_slurm_helpers,
        test_error_conditions,
        test_strict_mode,
    ]

    failed_tests = []

    for test_func in tests:
        try:
            test_func()
        except Exception as e:
            print(f"✗ {test_func.__name__} failed: {e}")
            failed_tests.append(test_func.__name__)
        print()

    print("=" * 40)
    if failed_tests:
        print(f"❌ {len(failed_tests)} test(s) failed: {', '.join(failed_tests)}")
        return 1
    else:
        print("✅ All tests passed!")
        return 0


if __name__ == "__main__":
    sys.exit(main())
