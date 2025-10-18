from pathlib import Path

import jinja2
from fabric import Connection

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


class RSyncSynchronizer:
    def __init__(
        self,
        local_workspace_location: str,
        remote_workspace_location: str,
        exclusion_file: str | None = None,
    ):
        self.source = local_workspace_location
        self.destination = remote_workspace_location
        self.exclusion_file = exclusion_file

    def synchronize_workspaces(self, conn: Connection):
        conn.run(f"mkdir -p {self.destination}", pty=False)

        self._run_rsync(conn, source=self.source, use_exclusion=True)

    def synchronize_file(self, conn: Connection, call_location: Path):
        self._run_rsync(conn, source=str(call_location), use_exclusion=False)

    def _run_rsync(self, conn: Connection, source: str, use_exclusion: bool = True):
        if not isinstance(conn.user, str):
            raise ValueError("Connection user is not set.")
        if not isinstance(conn.host, str):
            raise ValueError("Connection host is not set.")
        if not isinstance(conn.port, int):
            raise ValueError("Connection port is not set.")

        conn.local(
            compose_rsync_command(
                port=conn.port,
                user=conn.user,
                host=conn.host,
                source=source,
                destination=self.destination,
                exclusion_file=self.exclusion_file if use_exclusion else None,
            ),
            pty=False,
        )
