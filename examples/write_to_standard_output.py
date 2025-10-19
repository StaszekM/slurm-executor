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
        SerializeCall(),
        RSyncWorkspaceToRemote(
            source=".", destination="~/remote_jobs", exclusion_file="rsync-exclude.txt"
        ),
        SendCall(),
        ComposeSbatchScript(
            partition=cpu_partition,
            time="01:00:00",
        ),
        SendSbatchScript(),
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
