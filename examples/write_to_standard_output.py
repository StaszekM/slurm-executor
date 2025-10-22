import logging
import os

from dotenv import load_dotenv

from slurm_executor import Pipeline
from slurm_executor.hooks.CancelJobOnFailure import CancelJobHook
from slurm_executor.models.ConnectionConfig import ConnectionConfig
from slurm_executor.pipeline.RSyncWorkspace import RSyncWorkspace
from slurm_executor.pipeline.SendCall import (
    SendCall,
)
from slurm_executor.pipeline.SendSbatchScript import SendSbatchScript
from slurm_executor.pipeline.SubmitSbatchScript import SubmitSbatchScript
from slurm_executor.pipeline.WaitForJobCompletion import WaitForJobCompletion

logging.basicConfig(level=logging.INFO)
load_dotenv()

remote = os.getenv("SLURM_REMOTE")
port = os.getenv("SLURM_PORT")
user = os.getenv("SLURM_USERNAME")
cpu_partition = os.getenv("CPU_PARTITION")

assert remote
assert port
assert user
assert cpu_partition

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
    ],
    connection_config=ConnectionConfig(
        host=remote,
        user=user,
        port=int(port),
    ),
    hooks=[
        CancelJobHook(),
    ],
)


@pipeline.remote_run
def write_to_standard_output(text: str):
    print(f"Writing to standard output: {text}")


if __name__ == "__main__":
    write_to_standard_output("Hello!")
