"""
E2E tests for write_to_standard_output example.

Tests the complete workflow of:
1. Syncing code to remote cluster
2. Submitting SLURM job via the library
3. Executing Python function on cluster
4. Capturing standard output
5. Verifying job completion
"""

import os
import re
import subprocess
import sys
import time
from pathlib import Path

import pytest

# Add library to path for imports
REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT / "src"))

from slurm_executor import (
    ConnectionConfig,
    Pipeline,
    RSyncWorkspace,
    SendCall,
    SendSbatchScript,
    SubmitSbatchScript,
    WaitForJobCompletion,
)


class TestWriteToStandardOutputHappyPath:
    """
    Happy path tests for write_to_standard_output example.

    These tests verify that the basic workflow completes successfully
    when everything is configured correctly.
    """

    @pytest.fixture(autouse=True)
    def setup_environment(self, library_env, test_workspace):
        """Set up environment for each test."""
        # Set environment variables for the library
        for key, value in library_env.items():
            os.environ[key] = str(value)

        # Store original working directory
        self.original_cwd = Path.cwd()

        # Create a test-specific workspace directory
        self.test_dir = test_workspace / "outputs" / "write_to_stdout_test"
        self.test_dir.mkdir(parents=True, exist_ok=True)

        yield

        # Cleanup: restore working directory
        os.chdir(self.original_cwd)

    def test_basic_write_to_stdout(self, slurm_cluster, library_env, test_workspace):
        """
        Test basic write_to_standard_output functionality.

        This is the happy path test that verifies:
        1. Library can connect to cluster
        2. Code is synced to remote
        3. Job is submitted successfully
        4. Function executes and writes to stdout
        5. Output is captured in job output file
        """
        # Create working directory inside container
        remote_workspace = "/workspace/tests/e2e/fixtures/test-workspace/outputs/write_to_stdout_test"

        # Create the remote directory in container
        subprocess.run(
            [
                "docker",
                "exec",
                slurm_cluster,
                "mkdir",
                "-p",
                remote_workspace,
            ],
            check=True,
        )

        # Configure pipeline with SSH key authentication
        ssh_key_path = library_env.get("SSH_KEY_PATH")
        pipeline = Pipeline(
            steps=[
                RSyncWorkspace(
                    local_root=str(test_workspace),
                    remote_root=remote_workspace,
                    exclude_from=None,  # No exclusions for test
                    direction="to_remote",
                ),
                SendCall(),
                SendSbatchScript(
                    partition=library_env["CPU_PARTITION"],
                    time="00:05:00",
                    sbatch_script_template_location=str(REPO_ROOT / "sbatch_script.jinja"),
                ),
                SubmitSbatchScript(output_file_location=f"{remote_workspace}/job.out"),
                WaitForJobCompletion(poll_interval_ms=1000),
                RSyncWorkspace(
                    local_root=str(test_workspace),
                    remote_root=remote_workspace,
                    exclude_from=None,
                    direction="from_remote",
                ),
            ],
            connection_config=ConnectionConfig(
                host=library_env["SLURM_REMOTE"],
                user=library_env["SLURM_USERNAME"],
                port=int(library_env["SLURM_PORT"]),
                connect_kwargs={
                    "key_filename": ssh_key_path,
                },
            ),
        )

        # Define the function to execute remotely
        @pipeline.remote_run
        def write_to_standard_output(text: str):
            print(f"Writing to standard output: {text}")

        # Execute the function
        test_message = "Hello from E2E test!"
        write_to_standard_output(test_message)

        # Wait a bit for file sync to complete
        time.sleep(2)

        # Verify the job output file was created
        job_output_file = test_workspace / "outputs" / "write_to_stdout_test" / "job.out"
        assert job_output_file.exists(), f"Job output file not found at {job_output_file}"

        # Read and verify the output
        output_content = job_output_file.read_text()
        print(f"\n📄 Job output:\n{output_content}")

        # Verify the expected message is in the output
        assert f"Writing to standard output: {test_message}" in output_content, (
            f"Expected message not found in output. Output was:\n{output_content}"
        )

        # Verify job completed successfully (check for common SLURM completion indicators)
        # The output should not contain error indicators
        assert "error" not in output_content.lower() or "0 errors" in output_content.lower(), (
            f"Job appears to have errors. Output:\n{output_content}"
        )

    def test_multiple_messages_to_stdout(self, slurm_cluster, library_env, test_workspace):
        """
        Test that multiple print statements work correctly.

        Verifies that:
        1. Multiple print statements are captured
        2. Output order is preserved
        3. All messages appear in the output file
        """
        # Create working directory
        remote_workspace = "/workspace/tests/e2e/fixtures/test-workspace/outputs/write_to_stdout_test"

        # Configure pipeline
        ssh_key_path = library_env.get("SSH_KEY_PATH")
        pipeline = Pipeline(
            steps=[
                RSyncWorkspace(
                    local_root=str(test_workspace),
                    remote_root=remote_workspace,
                    exclude_from=None,
                    direction="to_remote",
                ),
                SendCall(),
                SendSbatchScript(
                    partition=library_env["CPU_PARTITION"],
                    time="00:05:00",
                    sbatch_script_template_location=str(REPO_ROOT / "sbatch_script.jinja"),
                ),
                SubmitSbatchScript(output_file_location=f"{remote_workspace}/job_multi.out"),
                WaitForJobCompletion(poll_interval_ms=1000),
                RSyncWorkspace(
                    local_root=str(test_workspace),
                    remote_root=remote_workspace,
                    exclude_from=None,
                    direction="from_remote",
                ),
            ],
            connection_config=ConnectionConfig(
                host=library_env["SLURM_REMOTE"],
                user=library_env["SLURM_USERNAME"],
                port=int(library_env["SLURM_PORT"]),
                connect_kwargs={
                    "key_filename": ssh_key_path,
                },
            ),
        )

        @pipeline.remote_run
        def write_multiple_messages():
            print("Message 1: Start")
            print("Message 2: Middle")
            print("Message 3: End")

        # Execute
        write_multiple_messages()

        # Wait for sync
        time.sleep(2)

        # Verify output
        job_output_file = test_workspace / "outputs" / "write_to_stdout_test" / "job_multi.out"
        assert job_output_file.exists(), f"Job output file not found at {job_output_file}"

        output_content = job_output_file.read_text()
        print(f"\n📄 Job output:\n{output_content}")

        # Check all messages are present
        assert "Message 1: Start" in output_content
        assert "Message 2: Middle" in output_content
        assert "Message 3: End" in output_content

        # Check order is preserved
        start_pos = output_content.find("Message 1: Start")
        middle_pos = output_content.find("Message 2: Middle")
        end_pos = output_content.find("Message 3: End")

        assert start_pos < middle_pos < end_pos, "Messages not in expected order"
