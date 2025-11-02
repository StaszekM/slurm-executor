"""
E2E Test Configuration and Fixtures for SLURM Docker Cluster Integration
"""

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest
from dotenv import load_dotenv

# Add src to path for safe_get_env utility
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
from slurm_executor.utils import safe_get_env

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

    print("\n🚀 Starting SLURM Docker cluster for E2E tests...")

    try:
        # Start containers
        subprocess.run(
            [
                "make",
                "up",
            ],
            check=True,
        )

        # Wait for containers to be ready
        print("⏳ Waiting for SLURM services to start...")
        time.sleep(10)

        # Check if containers are running
        result = subprocess.run(
            [
                "make",
                "status",
            ],
            capture_output=True,
            text=True,
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
            ["make", "register-cluster"],
            capture_output=True,
            text=True,
        )

        if register_result.returncode != 0:
            print(f"Warning: Cluster registration failed: {register_result.stderr}")
            print(f"Standard output: {register_result.stdout}")
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
                subprocess.run(["make", "collect-logs"], check=False)
            except Exception as e:
                print(f"Warning: Failed to collect logs: {e}")

        print("\n🧹 Cleaning up SLURM Docker cluster...")
        subprocess.run(
            [
                "make",
                "clean",
            ],
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

    # cleanup src dir if exists
    src_dir = workspace_path / "src"
    if src_dir.exists():
        shutil.rmtree(src_dir)

    # cleanup pyproject.toml if exists
    pyproject_file = workspace_path / "pyproject.toml"
    if pyproject_file.exists():
        pyproject_file.unlink()

    # cleanup .python-version if exists
    python_version_file = workspace_path / ".python-version"
    if python_version_file.exists():
        python_version_file.unlink()

    shutil.copytree(
        Path(__file__).parent.parent.parent / "src",
        workspace_path / "src",
    )

    shutil.copy(
        Path(__file__).parent.parent.parent / "pyproject.toml",  # noqa: E501
        workspace_path / "pyproject.toml",
    )

    shutil.copy(
        Path(__file__).parent.parent.parent / ".python-version",  # noqa: E501
        workspace_path / ".python-version",
    )

    return workspace_path


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
def ssh_key(slurm_cluster, library_env):
    """
    Generate SSH key for passwordless access to containers.

    This fixture:
    1. Generates an SSH key pair for testing
    2. Copies the public key to the container's authorized_keys
    3. Ensures the remote host key is present in local
       known_hosts to avoid host verification warnings
    4. Returns path to the private key
    5. Cleans up the key after tests complete
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
                "-q",
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
    docker_cmd = (
        f'mkdir -p /root/.ssh && echo "{pub_key}" >> '
        "/root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys"
    )
    subprocess.run(
        [
            "docker",
            "exec",
            slurm_cluster,
            "bash",
            "-c",
            docker_cmd,
        ],
        check=True,
    )

    # Ensure remote host key is present in known_hosts to avoid
    # host verification failures
    remote_host = library_env["SLURM_REMOTE"]
    remote_port = library_env["SLURM_PORT"]
    known_hosts_path = ssh_dir / "known_hosts"

    # Remove any existing entry for the host:port to avoid stale keys
    if known_hosts_path.exists():
        subprocess.run(
            [
                "ssh-keygen",
                "-q",
                "-f",
                str(known_hosts_path),
                "-R",
                f"[{remote_host}]:{remote_port}",
            ],
            check=False,
            capture_output=True,
            text=True,
        )

    # Retrieve current host key from remote and append to known_hosts
    try:
        scan = subprocess.run(
            ["ssh-keyscan", "-p", str(remote_port), remote_host],
            check=True,
            capture_output=True,
            text=True,
        )
        if scan.stdout:
            # Ensure known_hosts file exists
            known_hosts_path.parent.mkdir(parents=True, exist_ok=True)
            with known_hosts_path.open("a", encoding="utf-8") as f:
                f.write(scan.stdout)
            # Restrict permissions to a reasonable default
            try:
                known_hosts_path.chmod(0o644)
            except Exception:
                pass
    except Exception as exc:
        print(
            "Warning: ssh-keyscan failed to fetch host key for "
            f"{remote_host}:{remote_port}: {exc}"
        )

    print(f"✅ SSH key configured for container access, path: {key_path}")
    yield key_path

    # Cleanup
    if key_path.exists():
        key_path.unlink()
        key_path.with_suffix(".pub").unlink()

    # Remove our known_hosts entry to avoid polluting user's file
    try:
        if known_hosts_path.exists():
            subprocess.run(
                [
                    "ssh-keygen",
                    "-q",
                    "-f",
                    str(known_hosts_path),
                    "-R",
                    f"[{remote_host}]:{remote_port}",
                ],
                check=False,
            )
    except Exception:
        pass


@pytest.fixture(scope="session")
def library_env():
    """
    Provide environment configuration for slurm-executor library tests.

    This fixture sets up the environment variables needed for the library
    to connect to the test cluster via SSH.

    Returns:
        dict: Environment variables for library usage
    """
    return {
        "SLURM_REMOTE": safe_get_env(
            "SLURM_REMOTE", "SSH hostname for SLURM cluster (localhost for tests)"
        ),
        "SLURM_PORT": safe_get_env("SLURM_PORT", "SSH port for SLURM connection"),
        "SLURM_USERNAME": safe_get_env(
            "SLURM_USERNAME", "Username for SLURM SSH connection"
        ),
        "CPU_PARTITION": safe_get_env("CPU_PARTITION", "SLURM partition name"),
    }
