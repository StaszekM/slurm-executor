import logging
import os

from dotenv import load_dotenv

from slurm_executor import slurm_task

load_dotenv()


@slurm_task(
    remote=os.getenv("SLURM_REMOTE"),
    time="00:00:10",
    partition=str(os.getenv("CPU_PARTITION")),
    port=2222,
    user=os.getenv("SLURM_USERNAME"),
    workdir="~/remote_jobs",
)
def write_to_standard_output(text: str):
    print(f"Writing to standard output: {text}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    write_to_standard_output("Hello!")
