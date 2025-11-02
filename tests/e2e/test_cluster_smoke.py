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
from fabric import Connection


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

    @pytest.mark.smoke
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

    @pytest.mark.smoke
    def test_can_connect_with_ssh(self, library_env, ssh_key):
        """Test that we can SSH into the SLURM controller node via Fabric"""

        ssh_key_path = ssh_key

        port = library_env.get("SLURM_PORT")
        remote = library_env.get("SLURM_REMOTE")
        user = library_env.get("SLURM_USERNAME")

        with Connection(
            host=remote,
            user=user,
            port=port,
            connect_kwargs={"key_filename": str(ssh_key_path)},
        ) as conn:
            result = conn.local("echo Hi", pty=True, hide=True)
            result = conn.run("sinfo", pty=True, hide=True)
            assert result.ok, f"SSH command failed: {result.stderr}"
            assert "normal" in result.stdout, (
                "Expected 'normal' partition not found via SSH"
            )
