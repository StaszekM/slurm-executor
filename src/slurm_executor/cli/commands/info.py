from importlib.metadata import version

from rich import print


def run_info():
    print("Slurm Executor CLI")
    print(f"Version: {version('slurm_executor')}")
    print("A CLI tool to manage Slurm job submissions and templates.")
