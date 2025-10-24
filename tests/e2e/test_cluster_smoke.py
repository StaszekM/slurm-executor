"""
Smoke tests to verify SLURM Docker cluster is working correctly.

These tests validate that:
1. SLURM cluster is running and responsive
2. Job submission and execution works
3. File sharing between containers works
4. Basic SLURM commands function properly
"""

import subprocess

import pytest


class TestSlurmClusterSmoke:
    """Basic smoke tests for SLURM cluster functionality."""

    @pytest.mark.smoke
    def test_cluster_is_running(self, slurm_cluster):
        """Test that all SLURM services are running."""
        # Verify sinfo command works
        result = subprocess.run(
            ["docker", "exec", slurm_cluster, "sinfo"], capture_output=True, text=True
        )

        assert result.returncode == 0, f"sinfo failed: {result.stderr}"
        assert "normal" in result.stdout, "Expected 'normal' partition not found"
        assert "idle" in result.stdout, "No idle nodes found"

    @pytest.mark.smoke
    def test_basic_job_submission(self, slurm_cluster):
        """Test that basic job submission works."""
        # Submit a simple job
        result = subprocess.run(
            [
                "docker",
                "exec",
                slurm_cluster,
                "sbatch",
                "--wrap=echo 'Hello from SLURM job'",
                "--output=/data/test_job.out",
                "--wait",  # Wait for job completion
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, f"Job submission failed: {result.stderr}"

        # Check job output
        output_result = subprocess.run(
            ["docker", "exec", slurm_cluster, "cat", "/data/test_job.out"],
            capture_output=True,
            text=True,
        )

        assert output_result.returncode == 0, "Failed to read job output"
        assert "Hello from SLURM job" in output_result.stdout

    @pytest.mark.smoke
    def test_file_sharing_between_containers(self, slurm_cluster, test_workspace):
        """Test that files can be shared between host and containers."""
        # Create a test file on the host
        test_file = test_workspace / "test_file.txt"
        test_content = "Test content from host"
        test_file.write_text(test_content)

        # Read the file from within the container
        result = subprocess.run(
            ["docker", "exec", slurm_cluster, "cat", "/test-workspace/test_file.txt"],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, (
            f"Failed to read file from container: {result.stderr}"
        )
        assert test_content in result.stdout

    def test_job_creates_output_file(self, slurm_cluster, test_workspace):
        """Test that SLURM jobs can create files in shared workspace."""
        # Submit job that creates a file
        result = subprocess.run(
            [
                "docker",
                "exec",
                slurm_cluster,
                "sbatch",
                "--wrap=echo 'Job output' > /test-workspace/outputs/job_created_file.txt",
                "--wait",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, f"Job submission failed: {result.stderr}"

        # Check if file was created on host
        output_file = test_workspace / "outputs" / "job_created_file.txt"
        assert output_file.exists(), "Job did not create expected output file"

        content = output_file.read_text().strip()
        assert content == "Job output", f"Unexpected file content: {content}"

    def test_multiple_compute_nodes_available(self, slurm_cluster):
        """Test that multiple compute nodes are available."""
        # Check node information
        result = subprocess.run(
            [
                "docker",
                "exec",
                slurm_cluster,
                "sinfo",
                "-N",  # Show node-oriented format
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, f"sinfo -N failed: {result.stderr}"

        # Should have c1 and c2 nodes
        assert "c1" in result.stdout, "Node c1 not found"
        assert "c2" in result.stdout, "Node c2 not found"

    def test_job_accounting_database(self, slurm_cluster):
        """Test that job accounting database is working."""
        # Submit a simple job
        submit_result = subprocess.run(
            ["docker", "exec", slurm_cluster, "sbatch", "--wrap=sleep 1", "--wait"],
            capture_output=True,
            text=True,
        )

        assert submit_result.returncode == 0, (
            f"Job submission failed: {submit_result.stderr}"
        )

        # Check if job appears in accounting
        sacct_result = subprocess.run(
            [
                "docker",
                "exec",
                slurm_cluster,
                "sacct",
                "--format=JobID,State,ExitCode",
                "--noheader",
            ],
            capture_output=True,
            text=True,
        )

        assert sacct_result.returncode == 0, f"sacct failed: {sacct_result.stderr}"
        assert "COMPLETED" in sacct_result.stdout, (
            "No completed jobs found in accounting"
        )
