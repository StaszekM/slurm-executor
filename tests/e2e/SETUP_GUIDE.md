# E2E Test Setup Guide

This guide explains how to set up and run the end-to-end tests for slurm-executor library.

## Prerequisites

- Docker and Docker Compose installed
- Python 3.12+ with pytest
- SSH client
- At least 4GB free disk space for Docker images
- Internet connection for building images

## Quick Start

### 1. Build the SLURM Cluster

The first time you run tests, you need to build the Docker images:

```bash
cd tests/e2e
make build
```

This will:
- Build the base SLURM Docker cluster image (~5-10 minutes)
- Build the SSH-enabled slurmctld image
- Pull MariaDB image

**Note**: The base image build takes time as it compiles SLURM from source.

### 2. Run Tests

#### Run All Tests (Smoke + Library)
```bash
cd tests/e2e
make test
```

#### Run Smoke Tests Only (Fast)
```bash
cd tests/e2e
make test-smoke
```

#### Run Library Integration Tests Only
```bash
cd tests/e2e
pytest library/ -v
```

#### Run Specific Test
```bash
cd tests/e2e
pytest library/test_write_to_standard_output.py::TestWriteToStandardOutputHappyPath::test_basic_write_to_stdout -v
```

### 3. Clean Up

After testing, clean up Docker resources:

```bash
cd tests/e2e
make clean
```

## Architecture

### Container Setup

The test environment consists of 5 containers:

1. **mysql-test**: MariaDB for SLURM accounting
2. **slurmdbd-test**: SLURM database daemon
3. **slurmctld-test**: SLURM controller (with SSH server)
4. **c1-test**: Compute node 1
5. **c2-test**: Compute node 2

### SSH Access

The `slurmctld-test` container has SSH server enabled:
- Port 2222 on host → Port 22 in container
- Root access with SSH key authentication
- SSH keys are auto-generated during test setup

### File Sharing

Three types of mounts are configured:

1. **test-workspace**: `/test-workspace` in containers
   - For job output files
   - Shared across all containers

2. **Project root**: `/workspace` in containers
   - Entire project mounted for library access
   - Allows testing actual library code

3. **SLURM volumes**: Named volumes for persistence
   - etc_munge_test, etc_slurm_test
   - slurm_jobdir_test, var_log_slurm_test

## Test Structure

### Smoke Tests (`test_cluster_smoke.py`)

Basic validation tests that run quickly:
- Cluster is running
- Job submission works
- File sharing works
- Multiple nodes available

Run with: `make test-smoke`

### Library Tests (`library/`)

Integration tests using actual slurm-executor library:
- `test_write_to_standard_output.py`: Tests stdout capture
- More tests can be added following the same pattern

Run with: `pytest library/ -v`

## Extending Tests

### Adding a New Library Test

1. Create test file in `library/` directory:
```python
# library/test_new_example.py
class TestNewExampleHappyPath:
    @pytest.fixture(autouse=True)
    def setup_environment(self, library_env, test_workspace):
        # Setup code
        for key, value in library_env.items():
            os.environ[key] = str(value)
        yield
    
    def test_basic_functionality(self, slurm_cluster, library_env):
        # Test implementation
        pass
```

2. Run the new test:
```bash
pytest library/test_new_example.py -v
```

**Key Point**: Once containers are built, you can add/modify tests without rebuilding!

### Adding a New Smoke Test

Add test methods to `TestSlurmClusterSmoke` class in `test_cluster_smoke.py`:

```python
def test_new_feature(self, slurm_cluster):
    """Test description."""
    result = subprocess.run(
        ["docker", "exec", slurm_cluster, "sinfo"],
        capture_output=True, text=True
    )
    assert result.returncode == 0
```

## Troubleshooting

### Containers Won't Start

```bash
# Check container status
docker compose -f docker-compose.test.yml ps

# Check logs
docker compose -f docker-compose.test.yml logs

# Restart services
docker compose -f docker-compose.test.yml restart
```

### SSH Connection Issues

```bash
# Verify SSH is running in container
docker exec slurmctld-test ps aux | grep sshd

# Test SSH connection manually
ssh -p 2222 -i ~/.ssh/slurm_test_key root@localhost

# Check SSH key setup
docker exec slurmctld-test cat /root/.ssh/authorized_keys
```

### Job Submission Failures

```bash
# Check SLURM status
docker exec slurmctld-test sinfo

# View SLURM logs
docker exec slurmctld-test tail -100 /var/log/slurm/slurmctld.log

# Check node status
docker exec slurmctld-test scontrol show nodes
```

### Test Workspace Issues

```bash
# Check mount is working
docker exec slurmctld-test ls -la /test-workspace

# Verify permissions
docker exec slurmctld-test ls -la /test-workspace/outputs

# Clear workspace
rm -rf fixtures/test-workspace/outputs/*
```

### Port Conflicts

If port 2222 is already in use:

1. Edit `.env.test` to change `SLURM_PORT`
2. Update `docker-compose.test.yml` ports mapping
3. Rebuild and restart containers

## Performance Notes

### Build Times
- Base SLURM image: 5-10 minutes (first time only)
- SSH-enabled image: 1-2 minutes (first time only)
- Subsequent builds: Use cached layers (fast)

### Test Execution Times
- Smoke tests: 30-60 seconds
- Library integration test: 60-120 seconds per test
- Total test suite: 3-5 minutes

### Optimizing Test Speed

1. **Run smoke tests first**: `make test-smoke`
2. **Use markers**: `pytest -m smoke` or `pytest -m library`
3. **Run specific tests**: Target individual test functions
4. **Keep containers running**: Don't run `make clean` between test runs

## CI/CD Integration

For GitHub Actions or other CI systems:

```yaml
- name: Setup test environment
  run: |
    cd tests/e2e
    make build

- name: Run E2E tests
  run: |
    cd tests/e2e
    make test

- name: Collect logs on failure
  if: failure()
  run: |
    cd tests/e2e
    make collect-logs

- name: Cleanup
  if: always()
  run: |
    cd tests/e2e
    make clean
```

## Environment Variables

Key environment variables (defined in `.env.test`):

- `SLURM_TAG`: Git tag for SLURM version
- `SLURM_VERSION`: Image version tag
- `SLURM_REMOTE`: Container hostname (for internal access)
- `SLURM_REMOTE_SSH`: Hostname for SSH access (usually localhost)
- `SLURM_PORT`: SSH port (2222)
- `SLURM_USERNAME`: SSH username (root)
- `CPU_PARTITION`: SLURM partition name (normal)

## Advanced Usage

### Interactive Debugging

```bash
# Start containers manually
docker compose -f docker-compose.test.yml up -d

# Open shell in controller
make shell

# Run commands inside container
sinfo
squeue
sacct

# Exit shell
exit

# Stop containers when done
docker compose -f docker-compose.test.yml down
```

### Running Library Examples Directly

```bash
# From host, with containers running
cd /path/to/slurm-executor

# Set environment
export SLURM_REMOTE=localhost
export SLURM_PORT=2222
export SLURM_USERNAME=root
export CPU_PARTITION=normal

# Run example
python examples/write_to_standard_output.py
```

### Viewing Real-time Logs

```bash
# All containers
docker compose -f docker-compose.test.yml logs -f

# Specific container
docker logs -f slurmctld-test

# SLURM logs inside container
docker exec slurmctld-test tail -f /var/log/slurm/slurmctld.log
```

## Tips

1. **First run takes time**: Building images is slow but only happens once
2. **Tests are reusable**: Once built, run tests repeatedly without rebuilding
3. **Add tests freely**: New test files don't require container rebuilds
4. **Debug interactively**: Use `make shell` to explore the environment
5. **Clean periodically**: Run `make clean` to free up disk space

## Next Steps

After running tests successfully:

1. Review test output and logs
2. Add new test scenarios as needed
3. Extend to other examples (save_file, etc.)
4. Configure CI/CD pipeline
5. Document any issues or improvements
