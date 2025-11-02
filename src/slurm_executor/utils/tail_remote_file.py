import sys
import threading
import time
from typing import Any, Dict

import paramiko


def tail_remote_file(
    host: str,
    user: str,
    port: int,
    remote_path: str,
    stop_event: threading.Event,
    stats: Dict[str, Any],
    identity_file_path: str | None = None,
):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        hostname=host, username=user, port=port, key_filename=identity_file_path
    )

    # -n +1 means start from beginning; adjust if needed
    cmd = f"tail -n +1 -f {remote_path}"
    transport = ssh.get_transport()
    if transport is None:
        raise Exception("SSH transport is not established.")
    channel = transport.open_session()
    channel.exec_command(cmd)

    channel.setblocking(0)

    bytes_read = 0

    try:
        while not stop_event.is_set():
            if channel.recv_ready():
                data = channel.recv(4096)
                bytes_read += len(data)
                stats["bytes_read"] = bytes_read
                if not data:
                    break
                sys.stdout.write(data.decode(errors="replace"))
                sys.stdout.flush()
            elif channel.exit_status_ready():
                break
            else:
                time.sleep(0.1)
    finally:
        channel.close()
        ssh.close()
