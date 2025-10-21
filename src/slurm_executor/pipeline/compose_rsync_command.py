import jinja2

with open("src/slurm_executor/synchronizer/rsync_command.jinja") as f:
    RSYNC_TEMPLATE = jinja2.Template(f.read())


def compose_rsync_command(
    port: int,
    user: str,
    host: str,
    source: str,
    destination: str,
    exclusion_file: str | None = None,
):
    command = RSYNC_TEMPLATE.render(
        port=port,
        user=user,
        host=host,
        source=source,
        destination=destination,
        exclusion_file=exclusion_file,
    )
    return command
