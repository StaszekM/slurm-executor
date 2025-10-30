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
        # Collect logs before cleanup if in CI environment
        if os.getenv("CI") or os.getenv("GITHUB_ACTIONS"):
            print("\n📋 Collecting container logs for CI...")
            try:
                subprocess.run(
                    ["make", "collect-logs"], cwd=str(compose_file.parent), check=False
                )
            except Exception as e:
                print(f"Warning: Failed to collect logs: {e}")

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


@pytest.fixture(scope="session")
def ssh_key(slurm_cluster):
    """
    Generate SSH key for passwordless access to containers.

    This fixture:
    1. Generates an SSH key pair for testing
    2. Copies the public key to the container's authorized_keys
    3. Returns path to the private key
    4. Cleans up the key after tests complete
    """
    ssh_dir = Path.home() / ".ssh"
    ssh_dir.mkdir(exist_ok=True, mode=0o700)

    key_path = ssh_dir / "slurm_test_key"

    # Generate key if it doesn't exist
    if not key_path.exists():
        print("🔑 Generating SSH key for container access...")
        subprocess.run(
            [
                "ssh-keygen",
                "-t",
                "rsa",
                "-b",
                "2048",
                "-f",
                str(key_path),
                "-N",
                "",  # No passphrase
                "-C",
                "slurm-test-key",
            ],
            check=True,
        )

    # Copy public key to container
    pub_key = key_path.with_suffix(".pub").read_text()
    subprocess.run(
        [
            "docker",
            "exec",
            slurm_cluster,
            "bash",
            "-c",
            f'mkdir -p /root/.ssh && echo "{pub_key}" >> /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys',
        ],
        check=True,
    )

    print(f"✅ SSH key configured for container access")

    yield key_path

    # Cleanup
    if key_path.exists():
        key_path.unlink()
        key_path.with_suffix(".pub").unlink()


@pytest.fixture
def library_env(ssh_key):
    """
    Provide environment configuration for slurm-executor library tests.

    This fixture sets up the environment variables needed for the library
    to connect to the test cluster via SSH.

    Returns:
        dict: Environment variables for library usage
    """
    return {
        "SLURM_REMOTE": os.getenv("SLURM_REMOTE_SSH", "localhost"),
        "SLURM_PORT": os.getenv("SLURM_PORT", "2222"),
        "SLURM_USERNAME": os.getenv("SLURM_USERNAME", "root"),
        "CPU_PARTITION": os.getenv("CPU_PARTITION", "normal"),
        "SSH_KEY_PATH": str(ssh_key),
    }
