import os
import time
from typing import Any, Callable

import fabric


def responsive_tail(
    conn: fabric.Connection,
    path: str,
    poll_interval: float = 0.5,
    on_output_callback: Callable[[str], Any] | None = None,
):
    """
    Tail a remote file over Fabric responsively.
    Returns True if any content was seen.
    """
    last_size = 0

    while True:
        # check if file exists and get its size
        res = conn.run(
            f"ls {os.path.dirname(path)} > /dev/null && stat -c %s {path}",
            warn=True,
            hide=True,
        )
        if not res.ok:
            time.sleep(poll_interval)
            continue

        size = int(res.stdout.strip())
        if size > last_size:
            # read only new bytes
            res2 = conn.run(f"tail -c +{last_size + 1} {path}", warn=True, hide=True)
            output = res2.stdout
            if output.strip():
                print(output, end="")
                if on_output_callback:
                    on_output_callback(output)
            last_size = size

        time.sleep(poll_interval)
