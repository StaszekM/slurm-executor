# Library Integration Tests

This directory contains end-to-end tests that validate the slurm-executor library by running actual example code against the test SLURM cluster.

## Structure

Tests are organized by example and scenario:

```
library/
├── __init__.py
├── test_write_to_standard_output.py  # Tests for write_to_standard_output example
└── README.md                          # This file
```

## Test Organization

Each test file follows this pattern:

1. **Happy Path Tests**: Basic functionality works as expected
   - Job completes successfully
   - Output is captured correctly
   - Results are as expected

2. **Error Scenarios** (future): Edge cases and failure modes
   - Job timeouts
   - Connection failures
   - Invalid parameters

## Writing New Tests

### Basic Structure

```python
class TestExampleHappyPath:
    """Happy path tests for [example_name]."""
    
    @pytest.fixture(autouse=True)
    def setup_environment(self, library_env, test_workspace):
        """Set up environment for each test."""
        # Set environment variables
        for key, value in library_env.items():
            os.environ[key] = str(value)
        yield
    
    def test_basic_functionality(self, slurm_cluster, library_env, test_workspace):
        """Test basic example functionality."""
        # 1. Import library components
        # 2. Configure pipeline
        # 3. Define and execute function
        # 4. Verify results
```

### Key Points

1. **Use `library_env` fixture**: Provides SSH configuration
2. **Use `test_workspace` fixture**: Provides clean workspace
3. **Import from src**: Use `sys.path.insert(0, ...)` to import library
4. **Wait for sync**: Add `time.sleep(2)` after execution for file sync
5. **Verify output**: Check job output files for expected content

## Running Tests

### All Library Tests
```bash
cd tests/e2e
make test  # Builds containers and runs all tests
```

### Specific Test File
```bash
cd tests/e2e
pytest library/test_write_to_standard_output.py -v
```

### Single Test
```bash
cd tests/e2e
pytest library/test_write_to_standard_output.py::TestWriteToStandardOutputHappyPath::test_basic_write_to_stdout -v
```

## Extending Tests

To add tests for a new example:

1. Create new test file: `test_[example_name].py`
2. Import example-specific functionality
3. Follow the happy path → error scenarios pattern
4. Document expected behavior

### Example: Adding save_file Tests

```python
# test_save_file.py
class TestSaveFileHappyPath:
    def test_basic_file_save(self, slurm_cluster, library_env):
        # Test basic file saving
        pass
    
    def test_nested_directory_creation(self, slurm_cluster, library_env):
        # Test creating nested paths
        pass

class TestSaveFileErrorScenarios:
    def test_invalid_path(self, slurm_cluster, library_env):
        # Test error handling for invalid paths
        pass
```

## Fixtures Available

- **`slurm_cluster`**: Running SLURM cluster (container name)
- **`library_env`**: Environment variables for SSH connection
- **`ssh_key`**: Path to SSH private key for authentication
- **`test_workspace`**: Clean workspace directory
- **`docker_exec`**: Helper for running commands in containers

## Notes

- Tests use SSH to connect to containers (port 2222 on host)
- SSH keys are automatically generated and configured
- Each test gets a clean workspace in `test-workspace/outputs/`
- Job output files are synced back to local workspace
- Container must be rebuilt if Dockerfile changes
- Tests can be extended without rebuilding containers
