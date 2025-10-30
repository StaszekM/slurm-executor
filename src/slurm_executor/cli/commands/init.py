import importlib.resources as pkg_resources
from pathlib import Path

import git
import git.exc
from rich import print

from slurm_executor.utils import get_git_root


def run_init(force: bool):
    try:
        git_root = get_git_root(Path.cwd())
    except git.exc.InvalidGitRepositoryError:
        print("[red]Error:[/red] Current directory is not inside a git repository.")
        return

    with (
        pkg_resources.files("slurm_executor.templates")
        .joinpath("basic_sbatch_template.jinja")
        .open("r") as template_file
    ):
        template_content = template_file.read()

    destination_file = git_root / "sbatch_script.jinja"

    if destination_file.exists() and not force:
        print(
            f"[red]Warning:[/red] {destination_file} already exists. \
Use --force to overwrite."
        )
        return

    else:
        print(f"Creating SBATCH script template at {destination_file}.")

    with open(destination_file, "w") as f:
        f.write(template_content)
    print(f"SBATCH script template created at {destination_file}.")
