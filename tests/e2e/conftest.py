"""
E2E Test Configuration and Fixtures for SLURM Docker Cluster Integration
"""

import os
import subprocess
import time
from pathlib import Path

import pytest
from dotenv import load_dotenv

# Load test environment variables
env_path = Path(__file__).parent / ".env.test"
load_dotenv(env_path)


@pytest.fixture(scope="session")
def slurm_cluster():
    """
    Start SLURM cluster and ensure it's ready for testing.

    This fixture:
    1. Builds and starts the docker-compose cluster
    2. Waits for services to be ready
    3. Registers the cluster with SlurmDBD
    4. Yields control to tests
    5. Cleans up containers and volumes after tests
    """

    compose_file = Path(__file__).parent / "docker-compose.test.yml"

    print("\n🚀 Starting SLURM Docker cluster for E2E tests...")

    try:
        # Start containers
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(compose_file),
                "--env-file",
                str(env_path),
                "up",
                "-d",
            ],
            check=True,
            cwd=str(compose_file.parent),
        )

        # Wait for containers to be ready
        print("⏳ Waiting for SLURM services to start...")
        time.sleep(30)

        # Check if containers are running
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(compose_file),
                "--env-file",
                str(env_path),
                "ps",
                "--services",
                "--filter",
                "status=running",
            ],
            capture_output=True,
            text=True,
            cwd=str(compose_file.parent),
        )

        running_services = result.stdout.strip().split("\n")
        expected_services = {"mysql", "slurmdbd", "slurmctld", "c1", "c2"}

        if not expected_services.issubset(set(running_services)):
            raise RuntimeError(
                f"Not all services are running. Expected: {expected_services}, Running: {running_services}"  # noqa: E501
            )

        # Register cluster with SlurmDBD
        print("📝 Registering cluster with SlurmDBD...")
        register_result = subprocess.run(
            ["docker", "exec", "slurmctld-test", "/usr/local/bin/register_cluster.sh"],
            capture_output=True,
            text=True,
        )

        if register_result.returncode != 0:
            print(f"Warning: Cluster registration failed: {register_result.stderr}")
            # Continue anyway as the cluster might still be functional

        # Verify SLURM is working
        print("🔍 Verifying SLURM functionality...")
        sinfo_result = subprocess.run(
            ["docker", "exec", "slurmctld-test", "sinfo"],
            capture_output=True,
            text=True,
        )

        if sinfo_result.returncode == 0:
            print("✅ SLURM cluster is ready!")
            print(f"Cluster status:\n{sinfo_result.stdout}")
        else:
            print(f"⚠️  SLURM cluster might not be fully ready: {sinfo_result.stderr}")

        yield "slurmctld-test"

    finally:
        print("\n🧹 Cleaning up SLURM Docker cluster...")
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(compose_file),
                "--env-file",
                str(env_path),
                "down",
                "--rmi",
                "local",
                "--volumes",
            ],
            cwd=str(compose_file.parent),
        )


@pytest.fixture
def test_workspace():
    """
    Provide a clean test workspace directory.

    Returns:
        Path: Path to the test workspace directory
    """
    workspace_path = Path(__file__).parent / "fixtures" / "test-workspace"

    # Clean outputs directory before each test
    outputs_dir = workspace_path / "outputs"
    if outputs_dir.exists():
        for file in outputs_dir.glob("*"):
            if file.is_file():
                file.unlink()

    return workspace_path


@pytest.fixture
def slurm_env():
    """
    Provide SLURM environment configuration for tests.

    Returns:
        dict: Environment variables needed for SLURM connection
    """
    return {
        "SLURM_REMOTE": os.getenv("SLURM_REMOTE", "slurmctld-test"),
        "SLURM_PORT": os.getenv("SLURM_PORT", "22"),
        "SLURM_USERNAME": os.getenv("SLURM_USERNAME", "root"),
        "CPU_PARTITION": os.getenv("CPU_PARTITION", "normal"),
    }


@pytest.fixture
def docker_exec():
    """
    Helper fixture for executing commands in SLURM containers.

    Returns:
        callable: Function to execute commands in containers
    """

    def exec_command(container: str, command: str, **kwargs):
        """Execute command in specified container."""
        return subprocess.run(
            ["docker", "exec", container, "bash", "-c", command],
            capture_output=True,
            text=True,
            **kwargs,
        )

    return exec_command
