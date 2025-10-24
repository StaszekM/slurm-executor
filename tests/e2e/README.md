# E2E Testing for SLURM Executor

This directory contains end-to-end tests for the SLURM Executor library using a containerized SLURM cluster.

## Overview

The E2E tests validate the complete workflow of:
1. Deploying a SLURM cluster using Docker containers
2. Submitting jobs through the slurm-executor library
3. Verifying file synchronization and job execution
4. Testing error scenarios and edge cases

## Architecture

The test environment uses:
- **giovtorres/slurm-docker-cluster** (fork) as the base SLURM setup
- **Docker Compose** for orchestrating multiple containers
- **pytest** for test execution and fixtures
- **Shared volumes** for file exchange between host and containers

### Containers

| Container | Purpose | Hostname |
|-----------|---------|----------|
| mysql-test | Database for SLURM accounting | mysql |
| slurmdbd-test | SLURM database daemon | slurmdbd |
| slurmctld-test | SLURM controller | slurmctld |
| c1-test | Compute node 1 | c1 |
| c2-test | Compute node 2 | c2 |

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Python 3.9+ with pytest
- `uv` package manager (for running examples)

### Running Tests

```bash
# Navigate to E2E directory
cd tests/e2e

# Run all tests (includes building containers)
make test

# Run only smoke tests (faster)
make test-smoke

# Build containers without testing
make build

# Clean up containers and volumes
make clean
```

### Manual Testing

```bash
# Start the cluster manually
docker compose -f docker-compose.test.yml --env-file .env.test up --build

# Wait for startup (about 30 seconds)
sleep 30

# Register cluster
./register_cluster.sh

# Check cluster status
docker exec slurmctld-test sinfo

# Run tests
uv run python -m pytest -v

# Clean up
docker compose -f docker-compose.test.yml down --rmi local --volumes
```

## Test Structure

```
tests/e2e/
├── conftest.py              # pytest fixtures and setup
├── docker-compose.test.yml  # Docker Compose configuration
├── .env.test               # Environment variables
├── pytest.ini             # pytest configuration
├── Makefile               # Convenient commands
├── fixtures/
│   └── test-workspace/    # Shared workspace for tests
│       └── outputs/       # Output files from SLURM jobs
└── test_*.py              # Test files
```

## Key Fixtures

### `slurm_cluster` (session scope)
- Starts the SLURM Docker cluster
- Waits for all services to be ready
- Registers the cluster with SlurmDBD
- Provides container name for accessing the cluster
- Cleans up after all tests complete

### `test_workspace`
- Provides a clean workspace directory for each test
- Mounted at `/test-workspace` in containers
- Automatically cleans output files between tests

### `slurm_env`
- Environment variables for connecting to the test cluster
- Can be used with slurm-executor examples

## Writing Tests

### Basic Test Structure

```python
def test_my_feature(slurm_cluster, test_workspace):
    """Test description."""
    
    # Execute commands in SLURM cluster
    result = subprocess.run([
        "docker", "exec", slurm_cluster,
        "sinfo"
    ], capture_output=True, text=True)
    
    assert result.returncode == 0
    
    # Check files in workspace
    output_file = test_workspace / "outputs" / "my_file.txt"
    assert output_file.exists()
```

### Using slurm-executor Library

```python
def test_save_file_workflow(slurm_cluster, slurm_env):
    """Test complete save_file.py workflow."""
    
    # Set environment for slurm-executor
    os.environ.update(slurm_env)
    
    # Import and run the example
    from examples.save_file import save_to_file
    save_to_file("./outputs/test.txt")
    
    # Verify results...
```

## Debugging

### View Container Logs

```bash
# All containers
make logs

# Specific container
docker logs slurmctld-test

# Follow logs
docker logs -f slurmctld-test
```

### Shell Access

```bash
# Open shell in SLURM controller
make shell

# Or manually
docker exec -it slurmctld-test bash
```

### Check SLURM Status

```bash
# Inside container
sinfo                    # Node information
squeue                   # Job queue
sacct                    # Job accounting
scontrol show config     # Configuration
```

## Troubleshooting

### Containers Not Starting

```bash
# Check container status
docker compose -f docker-compose.test.yml ps

# Check logs for errors
docker compose -f docker-compose.test.yml logs
```

### SLURM Services Not Ready

```bash
# Check if all services are running
docker exec slurmctld-test systemctl status slurmd
docker exec c1-test systemctl status slurmd

# Check SLURM logs
docker exec slurmctld-test tail -f /var/log/slurm/slurmctld.log
```

### Tests Failing

1. Ensure containers are healthy: `docker compose ps`
2. Check if cluster is registered: `docker exec slurmctld-test sacctmgr show cluster`
3. Verify file permissions in test workspace
4. Check for port conflicts (MySQL, SSH)

## Performance Notes

- Initial container build takes 5-10 minutes
- Cluster startup takes 30-60 seconds
- Tests should clean up their own files
- Use `make clean` to reset completely

## Next Steps

Week 2 will add:
- SSH access for direct slurm-executor testing
- Volume mounts for code synchronization
- Integration with actual library examples