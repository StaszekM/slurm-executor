import jinja2

from slurm_executor.models.RsyncDirection import RsyncDirection

with open("src/slurm_executor/synchronizer/rsync_command.jinja") as f:
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
    )
    return command
