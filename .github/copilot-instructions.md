# SLURM Executor - AI Assistant Guidelines

## Project Overview
This is a Python library for transparent execution of heavy computational jobs on SLURM clusters. The core concept is wrapping functions with a decorator to offload execution to HPC resources while maintaining transparent local development experience.

## Architecture: Pipeline-Based Execution

The system uses a **Pipeline of Steps** pattern where each step has explicit dependencies:

```python
# Pipeline steps must declare what they require/provide
class Step(ABC):
    @property
    def requires(self) -> list[str]:  # Dependencies from previous steps
    @property  
    def provides(self) -> list[str]:  # What this step adds to context
```

Key pipeline flow in `src/slurm_executor/pipeline/`:
1. **RSyncWorkspace** - Sync local code to remote cluster
2. **SendCall** - Serialize function call using cloudpickle 
3. **SendSbatchScript** - Generate SLURM sbatch script from Jinja template
4. **SubmitSbatchScript** - Submit job to SLURM queue
5. **WaitForJobCompletion** - Poll job status via SLURM CLI
6. **RSyncWorkspace** - Sync results back to local

## Critical Context Pattern

The `Context` object (in `models/Context.py`) carries state through the pipeline:
- Function, args, kwargs to execute
- Connection to remote cluster (via Fabric)
- Remote paths (workspace, call pickle, sbatch script)
- Job metadata (ID, output file location)

Pipeline validation (`Pipeline.verify()`) ensures step dependencies are satisfied before execution.

## Serialization & Remote Execution

Uses **cloudpickle** for function serialization - can handle closures, lambdas, and complex objects that standard pickle cannot. The `CloudpickleExecutor` on the remote side deserializes and executes.

Template in `executor/sbatch_script.jinja` shows remote execution pattern:
- Auto-installs `uv` package manager if missing
- Runs `uv sync` to install dependencies from pyproject.toml
- Executes CloudpickleExecutor via `uv run`

## Environment Configuration

Uses `.env` file with variables:
- `SLURM_REMOTE` - Remote cluster hostname
- `SLURM_PORT`, `SLURM_USERNAME` - SSH connection details  
- `CPU_PARTITION` - SLURM partition name

## File Synchronization

Two-way rsync with include/exclude lists:
- `rsync-exclude.txt` - What NOT to sync to remote (`.git/`, `.venv/`, etc.)
- `rsync-include.txt` - What to sync back from remote (`outputs/`)

Direction controlled by `RSyncWorkspace` step parameter.

## Underlying CLI Commands

The pipeline executes specific shell commands via Fabric's `Connection`:

**File Synchronization** (`RSyncWorkspace`):
```bash
# Create remote directory
mkdir -p /home/user/remote_job/

# Sync files using rsync template (synchronizer/rsync_command.jinja)
rsync -e 'ssh -p 22' --delete --info=progress2 -az --exclude-from=rsync-exclude.txt ./  user@host:/home/user/remote_job/
```

**SLURM Job Submission** (`SubmitSbatchScript`):
```bash
# Submit job with parsable output and custom output file
cd /remote/workspace && sbatch --parsable --output=/home/user/remote_job/job.out /path/to/script.sbatch
```

**Job Status Monitoring** (`WaitForJobCompletion`):
```bash
# Get job state using sacct
sacct -j JOB_ID -X --format=JobID,State --noheader

# Get stdout path from job info
scontrol show job JOB_ID | grep StdOut

# Verify file exists (with Lustre cache flush)
ls /directory > /dev/null && test -f /path/to/file && echo 'exists' || echo 'not_exists'
```

## Development Patterns

**Step Implementation**: Each pipeline step inherits from `Step` base class. Always implement `requires`, `provides`, and `run(ctx)` methods.

**Connection Management**: Use Fabric `Connection` object stored in context. Steps access via `ctx._connection`.

**Path Handling**: Remote paths built incrementally through pipeline - workspace → call pickle → sbatch script.

**Error Handling**: Steps should assert required context state before proceeding.

## Key Files to Reference

- `examples/save_file.py` - Complete pipeline usage example
- `src/slurm_executor/pipeline/Pipeline.py` - Core pipeline orchestration
- `src/slurm_executor/executor/sbatch_script.jinja` - SLURM job template
- `src/slurm_executor/models/Context.py` - State management