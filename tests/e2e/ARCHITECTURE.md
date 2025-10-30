# E2E Test Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         Host Machine                             │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    Test Runner (pytest)                   │   │
│  │                                                            │   │
│  │  ┌──────────────────┐  ┌──────────────────────────────┐  │   │
│  │  │  Smoke Tests     │  │  Library Integration Tests   │  │   │
│  │  │  (Direct Exec)   │  │  (SSH Connection)            │  │   │
│  │  └──────────────────┘  └──────────────────────────────┘  │   │
│  └──────────────────────────────────────────────────────────┘   │
│                              │                                    │
│                              │ SSH (port 2222)                    │
│                              │ Docker Exec                        │
│                              ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              Docker Compose Environment                   │   │
│  │                                                            │   │
│  │  ┌────────────┐  ┌────────────┐  ┌─────────────────────┐│   │
│  │  │   MySQL    │  │  SlurmDBD  │  │    SlurmCTLD        ││   │
│  │  │   (Test)   │◄─┤   (Test)   │◄─┤   (Test + SSH)      ││   │
│  │  └────────────┘  └────────────┘  └─────────────────────┘│   │
│  │                                            │               │   │
│  │                                            │               │   │
│  │                       ┌────────────────────┴──────┐       │   │
│  │                       │                           │       │   │
│  │                  ┌────▼─────┐              ┌─────▼────┐  │   │
│  │                  │ Compute  │              │ Compute  │  │   │
│  │                  │ Node c1  │              │ Node c2  │  │   │
│  │                  │  (Test)  │              │  (Test)  │  │   │
│  │                  └──────────┘              └──────────┘  │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                   │
│  Shared Volumes:                                                 │
│  • test-workspace/ → /test-workspace (all containers)            │
│  • project-root/ → /workspace (all containers)                   │
└─────────────────────────────────────────────────────────────────┘
```

## Test Execution Flow

### Smoke Tests (Direct Execution)

```
┌──────────────┐
│ pytest start │
└──────┬───────┘
       │
       ▼
┌──────────────────────────┐
│ Start containers         │ (session fixture, once per session)
│ - docker compose up      │
│ - Wait for services      │
│ - Register cluster       │
└──────┬───────────────────┘
       │
       ▼
┌──────────────────────────┐
│ Run smoke tests          │
│                          │
│ docker exec slurmctld    │
│   ├─ sinfo              │
│   ├─ sbatch --wrap="..." │
│   └─ cat output.txt      │
└──────┬───────────────────┘
       │
       ▼
┌──────────────────────────┐
│ Verify results           │
│ - Check exit codes       │
│ - Validate output        │
└──────┬───────────────────┘
       │
       ▼
┌──────────────────────────┐
│ Cleanup (session end)    │
│ - docker compose down    │
│ - Remove volumes         │
└──────────────────────────┘
```

### Library Integration Tests (SSH Connection)

```
┌──────────────┐
│ pytest start │
└──────┬───────┘
       │
       ▼
┌──────────────────────────┐
│ Start containers         │ (session fixture)
│ - docker compose up      │
│ - Enable SSH server      │
└──────┬───────────────────┘
       │
       ▼
┌──────────────────────────┐
│ Setup SSH access         │ (session fixture)
│ - Generate SSH key       │
│ - Copy to container      │
│ - Configure auth         │
└──────┬───────────────────┘
       │
       ▼
┌──────────────────────────┐
│ Run library test         │ (per test)
│                          │
│ 1. Setup environment     │
│    - Set env vars        │
│    - Prepare workspace   │
└──────┬───────────────────┘
       │
       ▼
┌──────────────────────────────────────┐
│ 2. Configure pipeline                │
│                                      │
│    pipeline = Pipeline(              │
│      steps=[                         │
│        RSyncWorkspace(to_remote),    │───┐
│        SendCall(),                   │   │
│        SendSbatchScript(),           │   │
│        SubmitSbatchScript(),         │   │
│        WaitForJobCompletion(),       │   │
│        RSyncWorkspace(from_remote)   │   │
│      ],                              │   │
│      connection_config=SSH_CONFIG    │   │
│    )                                 │   │
└──────┬───────────────────────────────┘   │
       │                                    │
       ▼                                    │
┌──────────────────────────┐               │
│ 3. Define function       │               │
│                          │               │
│    @pipeline.remote_run  │               │
│    def my_func():        │               │
│        print("Hello")    │               │
└──────┬───────────────────┘               │
       │                                    │
       ▼                                    │
┌──────────────────────────┐               │
│ 4. Execute               │               │
│    my_func()             │               │
└──────┬───────────────────┘               │
       │                                    │
       ├─────────────────────────────────┐ │
       │                                  │ │
       ▼                   Pipeline Steps ▼ ▼
┌──────────────────────────────────────────────────┐
│ RSyncWorkspace (to_remote)                       │
│                                                  │
│ SSH → localhost:2222                             │
│ rsync ./test-workspace → /workspace/.../         │
└──────┬───────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────┐
│ SendCall                                         │
│                                                  │
│ SSH → localhost:2222                             │
│ Serialize function with cloudpickle → call.pkl   │
└──────┬───────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────┐
│ SendSbatchScript                                 │
│                                                  │
│ SSH → localhost:2222                             │
│ Generate sbatch script from template             │
│ Upload script to remote                          │
└──────┬───────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────┐
│ SubmitSbatchScript                               │
│                                                  │
│ SSH → localhost:2222                             │
│ Run: sbatch script.sbatch                        │
│ Capture: job_id                                  │
└──────┬───────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────┐
│ WaitForJobCompletion                             │
│                                                  │
│ SSH → localhost:2222                             │
│ Poll: sacct -j job_id                            │
│ Until: State = COMPLETED                         │
└──────┬───────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────┐
│ RSyncWorkspace (from_remote)                     │
│                                                  │
│ SSH → localhost:2222                             │
│ rsync /workspace/.../ → ./test-workspace         │
│ Includes: job.out with function output           │
└──────┬───────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────┐
│ 5. Verify results        │
│                          │
│ - Read job.out           │
│ - Check for expected txt │
│ - Validate no errors     │
└──────┬───────────────────┘
       │
       ▼
┌──────────────────────────┐
│ 6. Cleanup               │
│ - Clear test workspace   │
│ - Ready for next test    │
└──────────────────────────┘
```

## Container Communication

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Network (bridge)                   │
│                                                               │
│  ┌────────┐         ┌──────────┐         ┌──────────────┐   │
│  │ MySQL  │◄────────┤ SlurmDBD │◄────────┤  SlurmCTLD   │   │
│  │  3306  │         │   6819   │         │  6817, 22    │   │
│  └────────┘         └──────────┘         └──────┬───────┘   │
│                                                   │           │
│                                                   │ SLURM     │
│                                                   │ protocol  │
│                                        ┌──────────┴────────┐ │
│                                        │                   │ │
│                                   ┌────▼─────┐      ┌─────▼──┐
│                                   │ Compute  │      │Compute │
│                                   │ c1:6818  │      │c2:6818 │
│                                   └──────────┘      └────────┘
│                                                               │
└─────────────────────────────────────────────────────────────┘
                                    ▲
                                    │
                          Port mapping: 2222 → 22 (SlurmCTLD)
                                    │
                          SSH connection from host tests
```

## File System Layout

```
Host:
/home/runner/work/slurm-executor/slurm-executor/
├── src/                           # Library source
│   └── slurm_executor/
├── examples/                       # Example scripts
│   └── write_to_standard_output.py
└── tests/
    └── e2e/
        ├── fixtures/
        │   └── test-workspace/    # Mounted to containers
        │       └── outputs/        # Job outputs appear here
        └── library/
            └── test_*.py          # Test files

Containers (all):
/workspace/                         # Full project (read-only mount)
  ├── src/slurm_executor/          # Library available
  ├── examples/
  └── tests/e2e/fixtures/test-workspace/

Container (slurmctld, during job):
/workspace/tests/e2e/fixtures/test-workspace/outputs/test_name/
  ├── call.pkl                      # Serialized function
  ├── script.sbatch                 # SLURM batch script
  └── job.out                       # Job stdout/stderr

Job execution (on compute node):
/workspace/                         # Same mount
  └── tests/e2e/fixtures/test-workspace/outputs/test_name/
      └── job.out                   # Written during job

After rsync back to host:
tests/e2e/fixtures/test-workspace/outputs/test_name/
  └── job.out                       # Now on host, test can verify
```

## Data Flow

### Test to Container

```
Test Code                  SSH/Rsync              Container
┌─────────────┐           ┌──────────┐          ┌──────────────┐
│ Pipeline    │──rsync───▶│localhost │──────────▶│ SlurmCTLD    │
│ .remote_run │  :2222    │  :2222   │          │ /workspace/  │
└─────────────┘           └──────────┘          └──────────────┘
                                │
                                │ SSH commands
                                ▼
                          ┌──────────────┐
                          │ SLURM Queue  │
                          │ sbatch       │
                          └──────┬───────┘
                                 │
                                 │ Job dispatch
                                 ▼
                          ┌──────────────┐
                          │ Compute Node │
                          │ Execute pkl  │
                          │ Write stdout │
                          └──────────────┘
```

### Container to Test

```
Compute Node              SLURM                 Container             Test
┌──────────────┐         ┌──────────┐          ┌──────────────┐    ┌────────┐
│ Job finishes │────────▶│ sacct    │          │ SlurmCTLD    │    │ pytest │
│ job.out      │         │ COMPLETED│          │              │    │        │
└──────────────┘         └──────────┘          └──────┬───────┘    └───▲────┘
                                                       │                │
                                                       │ rsync          │
                                                       │ back           │
                                                       ▼                │
                                                ┌──────────────┐       │
                                                │ Host         │───────┘
                                                │ test-workspace/
                                                │ outputs/     │
                                                └──────────────┘
```

## Test Isolation

Each test gets:

```
Session Level (shared):
├── Docker containers (5)
│   ├── mysql-test
│   ├── slurmdbd-test
│   ├── slurmctld-test ◄── SSH enabled
│   ├── c1-test
│   └── c2-test
└── SSH key pair
    ├── ~/.ssh/slurm_test_key
    └── ~/.ssh/slurm_test_key.pub

Test Level (isolated):
├── Clean workspace
│   └── test-workspace/outputs/test_specific_dir/
├── Fresh environment variables
│   ├── SLURM_REMOTE=localhost
│   ├── SLURM_PORT=2222
│   ├── SLURM_USERNAME=root
│   └── SSH_KEY_PATH=~/.ssh/slurm_test_key
└── Independent SSH connection
    └── New Fabric Connection per test
```

## Extension Points

### Adding New Test

```
1. Create File
   └── library/test_new_example.py

2. Follow Pattern
   ├── Class: TestNewExampleHappyPath
   ├── Fixture: setup_environment (autouse=True)
   └── Methods: test_* functions

3. Use Fixtures
   ├── slurm_cluster → Container name
   ├── library_env → SSH config
   ├── test_workspace → Clean directory
   └── ssh_key → Key path

4. Run Test
   └── pytest library/test_new_example.py -v
```

### Adding New Scenario

```
Same File, New Class:
├── TestNewExampleHappyPath
│   ├── test_basic_functionality
│   └── test_advanced_feature
│
└── TestNewExampleErrorScenarios  ◄── NEW
    ├── test_timeout
    ├── test_invalid_config
    └── test_connection_failure
```

## Performance Characteristics

```
Action                          Time        Frequency
────────────────────────────────────────────────────
Build base SLURM image          8-10 min    Once ever
Build SSH image                 1-2 min     Once ever
Start containers                30-60 sec   Once per session
Generate SSH key                1-2 sec     Once per session
Run single library test         60-120 sec  Per test
  ├─ SSH connection             <1 sec      Per test
  ├─ Rsync to remote            2-5 sec     Per test
  ├─ Submit job                 1-2 sec     Per test
  ├─ Wait for completion        30-60 sec   Per test
  └─ Rsync from remote          2-5 sec     Per test
Stop containers                 5-10 sec    Once per session
Full test suite                 3-5 min     Per run
```

## Resource Usage

```
Component           Memory      Disk        CPU
──────────────────────────────────────────────
Base SLURM image    -           2-3 GB      -
MySQL container     256 MB      100 MB      1%
SlurmDBD container  128 MB      50 MB       1%
SlurmCTLD container 256 MB      100 MB      2%
Compute c1          512 MB      100 MB      varies
Compute c2          512 MB      100 MB      varies
Test workspace      -           10-50 MB    -
──────────────────────────────────────────────
Total (approx)      1.7 GB      3-4 GB      5-10%
```

## Security Considerations

**Test Environment Only - Not for Production**

- Root SSH access enabled
- Passwordless SSH keys
- No firewall restrictions
- Containers share volumes
- No secrets management

This is acceptable for testing but should never be used in production.

## Summary

The architecture provides:

1. **Realistic SLURM environment** - Full cluster with accounting
2. **SSH connectivity** - Library can connect like real deployment
3. **Isolated testing** - Each test independent
4. **Fast iteration** - No rebuild for test changes
5. **Clear flow** - Well-defined data paths
6. **Easy extension** - Add tests without infrastructure changes

The design balances realism (full SLURM cluster) with practicality (fast tests, easy debugging).
