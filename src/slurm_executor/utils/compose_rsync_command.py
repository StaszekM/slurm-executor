import importlib.resources as pkg_resources

import jinja2

from slurm_executor.models.RsyncDirection import RsyncDirection

with (
    pkg_resources.files("slurm_executor.templates")
    .joinpath("rsync_command.jinja")
    .open("r") as f
):
    RSYNC_TEMPLATE = jinja2.Template(f.read())


def compose_rsync_command(
    port: int,
    user: str,
    host: str,
    local_root: str,
    remote_root: str,
    direction: RsyncDirection,
    exclusion_file: str | None = None,
    inclusion_file: str | None = None,
    identity_file_path: str | None = None,
):
    command = RSYNC_TEMPLATE.render(
        port=port,
        user=user,
        host=host,
        local_root=local_root,
        remote_root=remote_root,
        exclusion_file=exclusion_file,
        inclusion_file=inclusion_file,
        direction=direction,
        identity_file_path=identity_file_path,
    )
    return command
