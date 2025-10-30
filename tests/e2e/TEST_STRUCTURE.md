# E2E Test Structure and Extensibility

This document explains the structure of the e2e tests and how they're designed for extensibility without requiring expensive rebuilds.

## Design Philosophy

The test infrastructure is designed with these principles:

1. **Separate Infrastructure from Tests**: Container setup is independent of test code
2. **Build Once, Test Many**: After initial build, tests can be added/modified without rebuilds
3. **Modular Test Organization**: Tests are organized by functionality and scenario
4. **Reusable Fixtures**: Common setup is abstracted into pytest fixtures

## Directory Structure

```
tests/e2e/
├── conftest.py                      # Core pytest fixtures (session-scoped)
├── docker-compose.test.yml          # Container orchestration
├── Dockerfile.ssh                   # Extended SLURM image with SSH
├── docker-entrypoint-ssh.sh         # SSH + SLURM startup script
├── .env.test                        # Environment configuration
├── pytest.ini                       # Pytest configuration and markers
├── Makefile                         # Convenient build/test commands
│
├── test_cluster_smoke.py            # Basic SLURM functionality tests
│
├── library/                         # Library integration tests
│   ├── __init__.py
│   ├── README.md                    # Library test documentation
│   ├── test_write_to_standard_output.py   # First library test
│   └── test_[example].py            # Future: Additional examples
│
├── fixtures/
│   └── test-workspace/              # Shared workspace for jobs
│       └── outputs/                 # Job output files
│
├── README.md                        # General E2E documentation
├── SETUP_GUIDE.md                   # Setup and running instructions
└── TEST_STRUCTURE.md                # This file
```

## Two-Layer Test Architecture

### Layer 1: Infrastructure (Rebuild Required)

Changes to these files require container rebuild:

- `docker-compose.test.yml` - Container configuration
- `Dockerfile.ssh` - Image definition
- `docker-entrypoint-ssh.sh` - Container startup
- `.env.test` - Environment variables (some changes)

**When to rebuild**: 
```bash
make clean
make build
```

### Layer 2: Tests (No Rebuild Required)

Changes to these files do NOT require rebuild:

- `test_*.py` - All test files
- `conftest.py` - Test fixtures (most changes)
- `pytest.ini` - Test configuration
- Test data files

**Just rerun tests**:
```bash
make test
# or
pytest library/test_new_test.py
```

## How Extensibility Works

### 1. Volume Mounts Enable Live Code

The project root is mounted into containers:

```yaml
volumes:
  - ../../:/workspace  # Entire project accessible in container
```

This means:
- Tests can import the library directly from `/workspace/src`
- No need to rebuild when library code changes
- No need to rebuild when test code changes

### 2. Session-Scoped Fixtures

The `slurm_cluster` fixture runs once per test session:

```python
@pytest.fixture(scope="session")
def slurm_cluster():
    # Start containers once
    # ... startup code ...
    yield "slurmctld-test"
    # Cleanup once
```

Benefits:
- Containers start once for all tests
- Tests run quickly after initial startup
- Multiple test files share the same cluster

### 3. Test-Scoped Fixtures

Individual test fixtures reset state:

```python
@pytest.fixture
def test_workspace():
    # Clean workspace for each test
    # ... cleanup code ...
    return workspace_path
```

Benefits:
- Each test starts with clean state
- Tests don't interfere with each other
- No rebuild needed to add test isolation

### 4. SSH Access Pattern

SSH connection is established per-test:

```python
def test_something(library_env):
    pipeline = Pipeline(
        connection_config=ConnectionConfig(
            host=library_env["SLURM_REMOTE"],
            # ... config ...
        )
    )
    # Test code
```

Benefits:
- Each test independently connects
- Connection failures don't affect other tests
- New tests follow the same pattern

## Adding New Tests: Examples

### Example 1: Add Happy Path Test to Existing File

```python
# Edit: library/test_write_to_standard_output.py

class TestWriteToStandardOutputHappyPath:
    # ... existing tests ...
    
    def test_new_scenario(self, slurm_cluster, library_env):
        """New test case."""
        # Test implementation
        pass
```

**Run it**:
```bash
pytest library/test_write_to_standard_output.py::TestWriteToStandardOutputHappyPath::test_new_scenario -v
```

**Rebuild required**: NO

### Example 2: Add New Test File for Different Example

```python
# Create: library/test_save_file.py

import os
import sys
from pathlib import Path
import pytest

class TestSaveFileHappyPath:
    @pytest.fixture(autouse=True)
    def setup_environment(self, library_env, test_workspace):
        for key, value in library_env.items():
            os.environ[key] = str(value)
        self.test_dir = test_workspace / "outputs" / "save_file_test"
        self.test_dir.mkdir(parents=True, exist_ok=True)
        yield
    
    def test_basic_file_save(self, slurm_cluster, library_env):
        """Test basic file saving."""
        sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))
        from slurm_executor import Pipeline, ConnectionConfig
        # ... test implementation ...
```

**Run it**:
```bash
pytest library/test_save_file.py -v
```

**Rebuild required**: NO

### Example 3: Add Error Scenario Tests

```python
# Create: library/test_write_to_standard_output.py (add new class)

class TestWriteToStandardOutputErrorScenarios:
    """Error and edge case tests."""
    
    @pytest.fixture(autouse=True)
    def setup_environment(self, library_env, test_workspace):
        # Same setup as happy path
        for key, value in library_env.items():
            os.environ[key] = str(value)
        yield
    
    def test_connection_timeout(self, library_env):
        """Test behavior when connection times out."""
        # Test implementation
        pass
    
    def test_invalid_partition(self, library_env):
        """Test behavior with invalid SLURM partition."""
        # Test implementation
        pass
```

**Run it**:
```bash
pytest library/test_write_to_standard_output.py::TestWriteToStandardOutputErrorScenarios -v
```

**Rebuild required**: NO

### Example 4: Add Custom Fixture

```python
# Edit: conftest.py

@pytest.fixture
def large_test_file(test_workspace):
    """Create a large file for testing."""
    file_path = test_workspace / "large_file.bin"
    with open(file_path, "wb") as f:
        f.write(b"x" * (10 * 1024 * 1024))  # 10MB file
    yield file_path
    file_path.unlink()  # Cleanup
```

**Use it in test**:
```python
def test_large_file_transfer(large_test_file, library_env):
    """Test transferring large files."""
    # Use large_test_file in test
    pass
```

**Rebuild required**: NO

## Test Execution Patterns

### Run All Tests
```bash
make test
```
- Runs smoke tests first
- Then runs library tests
- Total time: ~5 minutes

### Run Only Library Tests
```bash
make test-library
```
- Skips smoke tests
- Faster iteration
- Total time: ~3 minutes

### Run Single Test
```bash
pytest library/test_write_to_standard_output.py::TestWriteToStandardOutputHappyPath::test_basic_write_to_stdout -v
```
- Fastest iteration
- Total time: ~1-2 minutes

### Run Tests Matching Pattern
```bash
pytest -k "write_to" -v
```
- Runs all tests matching "write_to"
- Good for related tests

## When Rebuild IS Required

You need to rebuild containers when:

1. **Changing Docker images**
   - Modifying `Dockerfile.ssh`
   - Changing base image version
   - Adding system packages

2. **Changing container configuration**
   - Port mappings
   - Volume mounts (adding/removing)
   - Network configuration

3. **Changing SLURM configuration**
   - SLURM version upgrade
   - Partition configuration
   - Node configuration

**How to rebuild**:
```bash
make clean
make build
make test
```

## When Rebuild is NOT Required

You do NOT need to rebuild for:

1. **Adding new test files**
2. **Modifying existing tests**
3. **Adding test fixtures (usually)**
4. **Changing test data**
5. **Updating library code**
6. **Adding test markers**
7. **Changing test configuration**

**Just rerun tests**:
```bash
make test
# or
pytest library/ -v
```

## Performance Optimization Tips

### 1. Keep Containers Running During Development

```bash
# Start containers once
docker compose -f docker-compose.test.yml up -d

# Run tests multiple times
pytest library/test_write_to_standard_output.py -v
pytest library/test_save_file.py -v
# ... more test runs ...

# Stop containers when done
docker compose -f docker-compose.test.yml down
```

### 2. Use Test Markers

```python
# Mark tests
@pytest.mark.slow
def test_long_running():
    pass

@pytest.mark.quick
def test_fast():
    pass
```

```bash
# Run only quick tests
pytest -m quick

# Skip slow tests
pytest -m "not slow"
```

### 3. Run Tests in Parallel (Future Enhancement)

```bash
# Install pytest-xdist
pip install pytest-xdist

# Run tests in parallel
pytest -n auto library/
```

## Best Practices for Adding Tests

1. **Follow the Happy Path → Error Scenario pattern**
   - Start with happy path tests
   - Add error scenarios separately

2. **Use descriptive test names**
   ```python
   def test_basic_write_to_stdout()  # Good
   def test_feature1()               # Bad
   ```

3. **Document what you're testing**
   ```python
   def test_multiple_messages():
       """
       Test that multiple print statements work correctly.
       
       Verifies:
       1. Multiple prints are captured
       2. Order is preserved
       3. All messages appear in output
       """
   ```

4. **Keep tests independent**
   - Each test should work in isolation
   - Don't depend on test execution order

5. **Clean up after yourself**
   ```python
   @pytest.fixture
   def my_fixture():
       # Setup
       resource = create_resource()
       yield resource
       # Cleanup
       resource.cleanup()
   ```

## Summary

The e2e test structure is designed for maximum extensibility:

✅ **Add tests freely** - No rebuild required
✅ **Modify library code** - No rebuild required  
✅ **Change test fixtures** - Usually no rebuild required
✅ **Run tests quickly** - Containers stay running
✅ **Debug easily** - Direct container access

❌ **Change Docker config** - Rebuild required
❌ **Update system packages** - Rebuild required
❌ **Change SLURM version** - Rebuild required

This design allows rapid iteration on tests while maintaining a realistic SLURM environment.
