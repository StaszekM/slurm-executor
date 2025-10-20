import logging
import os

from dotenv import load_dotenv

from slurm_executor.pipeline.ConnectionConfig import ConnectionConfig
from slurm_executor.pipeline.Pipeline import Pipeline
from slurm_executor.pipeline.Step import (
    ComposeSbatchScript,
    RSyncWorkspaceToRemote,
    SendCall,
    SendSbatchScript,
    SerializeCall,
    SubmitSbatchScript,
    WaitForJobCompletion,
)

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
        RSyncWorkspaceToRemote(
            source=".", destination="~/remote_job", exclusion_file="rsync-exclude.txt"
        ),
        SerializeCall(),
        SendCall(),
        ComposeSbatchScript(
            partition=cpu_partition,
            time="01:00:00",
        ),
        SendSbatchScript(),
        SubmitSbatchScript(output_file_location="job.out"),
        WaitForJobCompletion(poll_interval_ms=5000),
    ],
    connection_config=ConnectionConfig(
        host=remote,
        user=user,
        port=int(port),
    ),
)


@pipeline.remote_run
def write_to_standard_output(text: str):
    print(f"Writing to standard output: {text}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    write_to_standard_output("Hello!")
