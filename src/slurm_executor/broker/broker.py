import logging
import pathlib
import tempfile
import time as libtime
from typing import Callable, ParamSpec, TypeVar

from slurm_executor.executor.CloudpickleExecutor import CloudpickleExecutor
from slurm_executor.synchronizer.RSyncSynchronizer import RSyncSynchronizer
from slurm_executor.utils.LoggableConnection import LoggableConnection

P = ParamSpec("P")
T = TypeVar("T")

logger = logging.getLogger(__name__)


def slurm_task(
    partition: str,
    time: str,
    workdir: str,
    port: int,
    remote: str | None = None,
    user: str | None = None,
):
    def decorator(func: Callable[P, T]) -> Callable[P, T | None]:
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T | None:
            if remote is None:
                # run locally for testing
                print(f"[local] Running {func.__name__}")
                return func(*args, **kwargs)

            # --- serialize call ---
            func_name = func.__name__
            workspace_location = f"{workdir}/{func_name}_{int(libtime.time())}"

            tmp = tempfile.TemporaryDirectory()
            local_job_dir = pathlib.Path(tmp.name)
            call_file = local_job_dir / "call.pkl"
            job_script_name = "job.sh"
            job_out_file = "job.out"
            local_script = local_job_dir / job_script_name

            executor = CloudpickleExecutor(
                serialize_to=call_file,
                deserialize_from="call.pkl",
                partition=partition,
                time=time,
                workspace_location=workspace_location,
            )
            executor.serialize_call(func, args, kwargs)
            executor.serialize_sbatch_script(local_script)

            synchronizer = RSyncSynchronizer(
                local_workspace_location="./",
                remote_workspace_location=workspace_location,
                exclusion_file="rsync-exclude.txt",
            )

            with LoggableConnection(remote, user=user, port=port) as conn:
                # --- rsync codebase to remote ---
                logger.info(f"Syncing to {remote}:{workspace_location}")
                synchronizer.synchronize_workspaces(conn)
                synchronizer.synchronize_file(conn, call_file)
                synchronizer.synchronize_file(conn, local_script)

                result = conn.run(
                    f"cd {workspace_location} && sbatch --output={job_out_file} {job_script_name}",  # noqa: E501
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
                        conn.run(f"cat {workspace_location}/{job_out_file}", hide=None)

                        break
                    elif out.startswith("RUNNING"):
                        print(f"[remote] Job {job_id} is still running... Cat:")
                        conn.run(f"cat {workspace_location}/{job_out_file}", hide=None)
                    libtime.sleep(5)
                return None

        return wrapper

    return decorator
