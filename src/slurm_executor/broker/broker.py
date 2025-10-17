import inspect
import os
import pathlib
import tempfile
import textwrap
import time as libtime
from typing import Callable, ParamSpec, TypeVar

from fabric import Connection

from slurm_executor.executor.CloudpickleExecutor import CloudpickleExecutor

P = ParamSpec("P")
T = TypeVar("T")


def slurm_task(
    partition: str = "short",
    time: str = "00:10:00",
    remote: str | None = None,
    workdir: str = "~/remote_jobs",
):
    def decorator(func: Callable[P, T]) -> Callable[P, T | None]:
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T | None:
            if remote is None:
                # run locally for testing
                print(f"[local] Running {func.__name__}")
                return func(*args, **kwargs)

            # --- serialize call ---
            func_name = func.__name__
            func_src = inspect.getsource(func)
            lines = func_src.splitlines()
            while lines and lines[0].lstrip().startswith("@"):
                lines.pop(0)
            func_src = textwrap.dedent("\n".join(lines))

            tmp = tempfile.TemporaryDirectory()
            local_job_dir = pathlib.Path(tmp.name)
            call_file = local_job_dir / "call.pkl"
            executor = CloudpickleExecutor(
                serialize_to=call_file, deserialize_from="call.pkl"
            )
            executor.serialize_call(func, args, kwargs)

            # --- rsync codebase to remote ---
            conn = Connection(remote)
            remote_path = f"{workdir}/{func_name}_{int(libtime.time())}"
            conn.run(f"mkdir -p {remote_path}")
            print(f"[remote] Syncing to {remote}:{remote_path}")

            os.system(
                f"rsync --delete --progress -az --exclude-from=rsync-exclude.txt ./ {remote}:{remote_path}/"  # noqa: E501
            )
            os.system(f"rsync {call_file} {remote}:{remote_path}/call.pkl")

            # --- submit job ---
            script = f"""#!/bin/bash
#SBATCH --partition={partition}
#SBATCH --time={time}

set -e
cd {remote_path}

source /usr/local/sbin/modules.sh

module load Python/3.12.3-GCCcore-13.3.0

uv sync

export PYTHONPATH={remote_path}:$PYTHONPATH

echo "Running"

uv run - <<'EOF'
from slurm_executor.executor.CloudpickleExecutor import CloudpickleExecutor

executor = CloudpickleExecutor(
    serialize_to="call.pkl", deserialize_from="call.pkl"
)
executor.run()

EOF
"""
            job_script_name = "job.sh"
            job_out_file = "job.out"
            local_script = local_job_dir / job_script_name
            local_script.write_text(script)

            os.system(f"rsync {local_script} {remote}:{remote_path}/{job_script_name}")

            result = conn.run(
                f"cd {remote_path} && sbatch --output={job_out_file} {job_script_name}",
                hide=None,
            )
            job_id = result.stdout.strip().split()[-1]
            print(f"[remote] Submitted job {job_id}")

            # --- simple polling until job completes ---
            while True:
                out = conn.run(
                    f"sacct -j {job_id} --format=State --noheader", hide=True
                ).stdout.strip()
                if (
                    out.startswith("COMPLETED")
                    or out.startswith("FAILED")
                    or out.startswith("CANCELLED")
                ):
                    print(f"[remote] Job {job_id} finished: {out}")
                    conn.run(f"cat {remote_path}/{job_out_file}", hide=None)

                    break
                libtime.sleep(5)
            return None

        return wrapper

    return decorator
