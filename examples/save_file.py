import logging
import os
import random

from dotenv import load_dotenv

from slurm_executor import Pipeline
from slurm_executor.models.ConnectionConfig import ConnectionConfig
from slurm_executor.pipeline.ExecuteCommand import ExecuteCommand
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
            sbatch_script_template_location="sbatch_script.jinja",
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


@pipeline.remote_run
def save_to_file(filename: str):
    random_number = random.randint(0, 10)

    print(f"Writing number {random_number} to file: {filename}")

    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, "w") as f:
        f.write(f"{random_number}\n")


if __name__ == "__main__":
    save_to_file("./outputs/file.txt")
