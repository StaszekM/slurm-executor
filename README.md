
> [!WARNING]  
>This project is under heavy development and it is in very early stage. It may contain a ton of bugs!

![Slurm Executor robot](doc/executor_robot.png)

# A library for transparent execution of heavy Python jobs on SLURM!

Imagine being able to offload **any**\* Python function to a HPC cluster with just few lines of code!

No more logging into a cluster, git pulling, troubleshooting and other headaches.

(\* or at least any function that `cloudpickle` can serialize)

Just define your expected pipeline:

```python
remote = os.getenv("SLURM_REMOTE")
port = os.getenv("SLURM_PORT")
user = os.getenv("SLURM_USERNAME")
cpu_partition = os.getenv("CPU_PARTITION")

pipeline = Pipeline(
    steps=[
        RSyncWorkspace(
            local_root="./",
            remote_root=f"/home/{user}/remote_job/",
            exclude_from="rsync-exclude.txt",
            direction="to_remote",
        ),
        SendCall(),
        SendSbatchScript(
            partition=cpu_partition,
            time="00:05:00",
        ),
        SubmitSbatchScript(output_file_location=f"/home/{user}/remote_job/job.out"),
        WaitForJobCompletion(poll_interval_ms=1000),
        ExecuteCommand(remote_command=f"ls /home/{user}/remote_job/"),
        RSyncWorkspace(
            local_root="./",
            remote_root=f"/home/{user}/remote_job/",
            include_only="rsync-include.txt",
            direction="from_remote",
        ),
    ],
    connection_config=ConnectionConfig(
        host=remote,
        user=user,
        port=int(port),
    ),
)
```

Decorate your function:

```python
from your_code import heavy_data_load, heavy_data_process, heavy_data_save
import os

@pipeline.remote_run
def heavy_step(input_file, output_file):
    data = heavy_data_load()
    processed = heavy_data_process(data)
    heavy_data_save(remote=os.getenv('s3://my_bucket'))

```

And watch how seamlessly you can execute remote jobs without leaving your local IDE!

## Core features:

- Blazingly fast repository synchronization between machines using `rsync` compression
- Safe serialization of functions with `cloudpickle`
- Automatic transferring of logs from remote job right into your local terminal
- Deep integration with familiar SLURM CLI: `sbatch`, `sinfo`, `sacct`
- Full customization of sbatch script, job resources, setting up environment and remote execution
- Easy pipeline composition to satisfy various use cases and HPC cluster setups.
- And more to come...!

