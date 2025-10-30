# E2E Tests Quick Start

Quick reference for getting started with the e2e tests.

## First Time Setup

```bash
cd tests/e2e

# Build containers (takes 5-10 minutes)
make build

# Run all tests
make test
```

## Regular Usage

```bash
cd tests/e2e

# Run all tests
make test

# Run only smoke tests (fast)
make test-smoke

# Run only library tests
make test-library

# Run specific test file
pytest library/test_write_to_standard_output.py -v

# Run specific test
pytest library/test_write_to_standard_output.py::TestWriteToStandardOutputHappyPath::test_basic_write_to_stdout -v
```

## Adding New Tests

### Add test to existing file (NO rebuild needed)
```python
# Edit: library/test_write_to_standard_output.py
def test_new_scenario(self, slurm_cluster, library_env):
    """Test new scenario."""
    # Implementation
```

```bash
pytest library/test_write_to_standard_output.py -v
```

### Add new test file (NO rebuild needed)
```python
# Create: library/test_new_example.py
class TestNewExampleHappyPath:
    @pytest.fixture(autouse=True)
    def setup_environment(self, library_env, test_workspace):
        for key, value in library_env.items():
            os.environ[key] = str(value)
        yield
    
    def test_basic(self, slurm_cluster, library_env):
        # Test implementation
```

```bash
pytest library/test_new_example.py -v
```

## Troubleshooting

### Containers won't start
```bash
docker compose -f docker-compose.test.yml logs
make clean
make build
```

### Tests fail
```bash
# Check cluster status
docker exec slurmctld-test sinfo

# View logs
docker exec slurmctld-test tail -100 /var/log/slurm/slurmctld.log

# Open shell for debugging
make shell
```

### Clean everything
```bash
make clean
docker system prune -f
```

## File Structure

```
tests/e2e/
├── test_cluster_smoke.py          # Basic SLURM tests
├── library/
│   └── test_write_to_standard_output.py  # Library tests
├── conftest.py                    # Pytest fixtures
├── docker-compose.test.yml        # Container setup
└── Makefile                       # Build/test commands
```

## Key Fixtures

- `slurm_cluster`: Running SLURM cluster
- `library_env`: Environment for SSH connection
- `test_workspace`: Clean workspace for tests
- `ssh_key`: Auto-generated SSH key

## Test Pattern

```python
class TestExampleHappyPath:
    @pytest.fixture(autouse=True)
    def setup_environment(self, library_env, test_workspace):
        for key, value in library_env.items():
            os.environ[key] = str(value)
        yield
    
    def test_feature(self, slurm_cluster, library_env):
        sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))
        from slurm_executor import Pipeline, ConnectionConfig
        
        pipeline = Pipeline(
            steps=[...],
            connection_config=ConnectionConfig(
                host=library_env["SLURM_REMOTE"],
                user=library_env["SLURM_USERNAME"],
                port=int(library_env["SLURM_PORT"]),
                connect_kwargs={"key_filename": library_env["SSH_KEY_PATH"]},
            ),
        )
        
        @pipeline.remote_run
        def my_function():
            # Function to run on cluster
            pass
        
        my_function()
        
        # Verify results
```

## Important Notes

✅ **No rebuild needed** when:
- Adding new test files
- Modifying test code
- Changing library code
- Adding fixtures

❌ **Rebuild required** when:
- Changing docker-compose.yml
- Modifying Dockerfile.ssh
- Updating SLURM version
- Changing system packages

## Documentation

- `SETUP_GUIDE.md` - Detailed setup and troubleshooting
- `TEST_STRUCTURE.md` - Architecture and extensibility
- `library/README.md` - Library test documentation
- `README.md` - Overview

## Help

```bash
make help
```

Shows all available commands.
