import threading
import time

from slurm_executor.models.Context import Context
from slurm_executor.models.Step import Step
from slurm_executor.slurm_cli.get_job_state import get_job_state
from slurm_executor.slurm_cli.get_job_stdout_path import get_job_stdout_path
from slurm_executor.slurm_cli.verify_file_availability import verify_file_availability
from slurm_executor.utils.tail_remote_file import tail_remote_file


class WaitForJobCompletion(Step):
    @property
    def provides(self) -> list[str]:
        return []

    @property
    def requires(self) -> list[str]:
        return ["job_id", "remote_workspace_path", "job_output_file_location"]

    def __init__(self, poll_interval_ms: int) -> None:
        super().__init__()
        self.poll_interval_ms = poll_interval_ms

    def run(self, ctx: Context):
        conn = ctx._connection
        job_id = ctx.job_id
        remote_workspace_path = ctx.remote_workspace_path
        output_file_location = ctx.job_output_file_location

        assert job_id is not None, (
            f"Job ID must be set in context before executing {type(self).__name__}."
        )
        assert remote_workspace_path is not None, (
            f"Remote workspace path must be set in context before executing {type(self).__name__}."
        )
        assert output_file_location is not None, (
            f"Job output file location must be set in context before executing {type(self).__name__}."
        )
        prev_state = None
        output_file_detected = False
        output_file = get_job_stdout_path(conn, job_id)

        stats = {}

        while True:
            if not output_file_detected:
                output_file_detected = verify_file_availability(
                    conn, remote_workspace_path, output_file
                )
                if output_file_detected:
                    print(
                        f"[monitor] job {job_id} output file detected at {output_file}"
                    )
                    stop_event = threading.Event()
                    t = threading.Thread(
                        target=tail_remote_file,
                        args=(
                            ctx.connection_config.host,
                            ctx.connection_config.user,
                            ctx.connection_config.port,
                            output_file,
                            stop_event,
                            stats,
                        ),
                        daemon=True,
                    )
                    t.start()

            state = get_job_state(conn, job_id)

            if state != prev_state:
                print(f"[monitor] job {job_id} state changed: {prev_state} -> {state}")
            elif prev_state == "PENDING":
                print(".", end="", flush=True)
            if state in {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"}:
                stop_event.set()
                t.join()
                # write remaining bytes
                if stats.get("bytes_read", 0) > 0:
                    conn.run(f"tail -c +{stats['bytes_read'] + 1} {output_file}")
                else:
                    conn.run(f"cat {output_file}")

                if state != "COMPLETED":
                    raise Exception(f"Job {job_id} failed with state {state}.")
                break
            prev_state = state
            time.sleep(self.poll_interval_ms / 1000.0)

        return ctx
