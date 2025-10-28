from pathlib import Path

import typer
import yaml
from rich import print

from slurm_executor.utils import get_git_root

app = typer.Typer(help="Slurm Executor CLI", no_args_is_help=True)


@app.command()
def info():
    """Display information about the Slurm Executor installation."""
    print("Slurm Executor CLI")
    print("Version: 1.0.0")
    print("A CLI tool to manage Slurm job submissions and templates.")


@app.command()
def init():
    """Initialize the Slurm Executor environment and SBATCH script templates."""

    cli_location = Path(__file__)

    git_root = get_git_root(cli_location)
    print(f"Git root directory: {git_root}")

    with open(git_root / "slurm-executor-config.yaml", "w") as conf_file:
        conf = dict(env_provider="uv")
        yaml.dump(conf, conf_file)


def run_cli():
    app()
