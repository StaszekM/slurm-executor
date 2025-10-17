import os

from dotenv import load_dotenv

from slurm_executor import slurm_task

load_dotenv()
print(os.getenv("SLURM_USERNAME"))


@slurm_task(
    remote=os.getenv("SLURM_REMOTE"),
    time="00:00:10",
    partition=str(os.getenv("CPU_PARTITION")),
    port=2222,
    user=os.getenv("SLURM_USERNAME"),
    connect_kwargs={
        "password": os.getenv("PASSWORD"),
    },
)
def write_to_standard_output(text: str):
    print(f"Writing to standard output: {text}")


if __name__ == "__main__":
    write_to_standard_output("Hello!")
