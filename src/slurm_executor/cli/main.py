import typer

from slurm_executor.cli.commands import run_info, run_init

app = typer.Typer(
    help="Slurm Executor CLI", no_args_is_help=True, pretty_exceptions_show_locals=False
)


@app.command()
def info():
    """Display information about the Slurm Executor installation."""
    run_info()


@app.command()
def init(
    force: bool = typer.Option(
        False, "--force", "-f", help="Overwrite existing files."
    ),
):
    """Initialize the SBATCH script templates."""
    run_init(force)


def run_cli():
    app()
