import os
import pathlib
import sys
import tempfile
import termios
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, ParamSpec, TypeVar

import cloudpickle
import jinja2
from fabric import Connection
from invoke.exceptions import UnexpectedExit
from paramiko.ssh_exception import SSHException

from slurm_executor.models.SerializableCallData import SerializableCallData
from slurm_executor.pipeline.Context import Context
from slurm_executor.utils import responsive_tail

P = ParamSpec("P")
T = TypeVar("T")


class Step(ABC):
    @abstractmethod
    def run(self, ctx: Context) -> Context:
        pass


class SerializeCall(Step):
    def __init__(self, serialize_to: Optional[Path] = None) -> None:
        super().__init__()

        self.serialize_to = serialize_to

    def run(self, ctx: Context):
        func = ctx.function
        args = ctx.args
        kwargs = ctx.kwargs

        call_data = SerializableCallData(
            func=func,
            args=args,
            kwargs=kwargs,
        )

        if self.serialize_to is not None:
            with open(self.serialize_to, "wb") as f:
                cloudpickle.dump(call_data, f)
            ctx.serialized_call_path = self.serialize_to
        else:
            tmp = tempfile.TemporaryDirectory(delete=False)
            local_job_dir = pathlib.Path(tmp.name)
            call_file = local_job_dir / "call.pkl"
            with open(call_file, "wb") as f:
                cloudpickle.dump(call_data, f)
            ctx.serialized_call_path = call_file
        return ctx


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


class RSyncWorkspaceToRemote(Step):
    def __init__(
        self, source: str, destination: str, exclusion_file: str | None = None
    ) -> None:
        super().__init__()
        self.source = source
        self.destination = destination
        self.exclusion_file = exclusion_file

    def run(self, ctx: Context):
        conn = ctx._connection
        host = ctx.connection_config.host
        user = ctx.connection_config.user
        port = ctx.connection_config.port

        conn.run(
            f"mkdir -p {self.destination}",
            pty=False,
        )

        conn.local(
            compose_rsync_command(
                port=port,
                user=user,
                host=host,
                source=self.source,
                destination=self.destination,
                exclusion_file=self.exclusion_file,
            ),
            pty=False,
        )

        ctx.remote_workspace_path = self.destination

        return ctx


with open("src/slurm_executor/executor/sbatch_script.jinja") as f:
    SBATCH_TEMPLATE = jinja2.Template(f.read())


def compose_sbatch_script(
    partition: str, time: str, workspace_location: str, remote_call_location: str
) -> str:
    return SBATCH_TEMPLATE.render(
        partition=partition,
        time=time,
        workspace_location=workspace_location,
        remote_call_location=remote_call_location,
    )


class ComposeSbatchScript(Step):
    def __init__(
        self,
        partition: str,
        time: str,
        serialize_to: Optional[Path] = None,
    ) -> None:
        super().__init__()
        self.partition = partition
        self.time = time

        self.serialize_to = serialize_to

        with open("src/slurm_executor/executor/sbatch_script.jinja") as f:
            self.sbatch_template = jinja2.Template(f.read())

    def run(self, ctx: Context):
        workspace_location = ctx.remote_workspace_path
        remote_call_location = ctx.remote_call_path
        assert workspace_location is not None, (
            f"Remote workspace location must be set in context before executing \
{type(self).__name__}."
        )
        assert remote_call_location is not None, (
            f"Remote call location must be set in context before executing \
{type(self).__name__}."
        )
        composed_file_contents = compose_sbatch_script(
            partition=self.partition,
            time=self.time,
            workspace_location=workspace_location,
            remote_call_location=remote_call_location,
        )

        if self.serialize_to is not None:
            with open(self.serialize_to, "w") as f:
                f.write(composed_file_contents)
            ctx.serialized_sbatch_path = self.serialize_to
        else:
            tmp = tempfile.TemporaryDirectory(delete=False)
            local_job_dir = pathlib.Path(tmp.name)
            job_script_name = "job.sh"
            local_script = local_job_dir / job_script_name
            with open(local_script, "w") as f:
                f.write(composed_file_contents)
            ctx.serialized_sbatch_path = local_script

        return ctx


class SendCall(Step):
    def run(self, ctx: Context):
        conn = ctx._connection
        call_location = ctx.serialized_call_path
        assert call_location is not None, (
            f"Call location must be set in context before executing {type(self).__name__}."
        )
        assert ctx.remote_workspace_path is not None, (
            f"Remote workspace location must be set in context before executing \
{type(self).__name__}."
        )

        remote_call_location = ctx.remote_workspace_path + "/" + call_location.name

        conn.local(
            compose_rsync_command(
                port=ctx.connection_config.port,
                user=ctx.connection_config.user,
                host=ctx.connection_config.host,
                source=str(call_location),
                destination=remote_call_location,
                exclusion_file=None,
            ),
            pty=False,
        )

        ctx.remote_call_path = call_location.name

        return ctx


class SendSbatchScript(Step):
    def run(self, ctx: Context):
        conn = ctx._connection
        sbatch_location = ctx.serialized_sbatch_path
        remote_workspace_path = ctx.remote_workspace_path
        assert sbatch_location is not None, (
            f"Sbatch script location must be set in context before executing {type(self).__name__}."
        )
        assert remote_workspace_path is not None, (
            f"Remote workspace location must be set in context before executing \
{type(self).__name__}."
        )

        remote_sbatch_location = remote_workspace_path + "/" + sbatch_location.name

        conn.local(
            compose_rsync_command(
                port=ctx.connection_config.port,
                user=ctx.connection_config.user,
                host=ctx.connection_config.host,
                source=str(sbatch_location),
                destination=remote_sbatch_location,
                exclusion_file=None,
            ),
            pty=False,
        )

        ctx.remote_sbatch_path = remote_sbatch_location

        return ctx


class SubmitSbatchScript(Step):
    def __init__(self, output_file_location: str) -> None:
        super().__init__()
        self.output_file_location = output_file_location

    def run(self, ctx: Context):
        conn = ctx._connection
        remote_sbatch_path = ctx.remote_sbatch_path
        remote_workspace_path = ctx.remote_workspace_path
        assert remote_workspace_path is not None, (
            f"Remote workspace location must be set in context before executing {type(self).__name__}."
        )
        assert remote_sbatch_path is not None, (
            f"Remote sbatch script location must be set in context before executing {type(self).__name__}."
        )

        output = conn.run(
            f"cd {remote_workspace_path} && sbatch --parsable --output={self.output_file_location} {remote_sbatch_path}",
            pty=False,
            hide=None,
        )
        job_id = output.stdout.split(";")[-1].strip()

        ctx.job_output_file_location = self.output_file_location
        ctx.job_id = job_id

        return ctx


class OutputTracker:
    def __init__(self):
        self.got_output = False

    def write(self, s):
        if s.strip():  # detect non-empty output
            self.got_output = True
        print(s, end="", flush=True)  # forward to stdout

    def flush(self):
        pass  # required for file-like interface


class WaitForJobCompletion(Step):
    def __init__(self, poll_interval_ms: int) -> None:
        super().__init__()
        self.poll_interval_ms = poll_interval_ms
        self.output_tracker = OutputTracker()

    def tail_log(self, ctx: Context, tail_process_marker: str):
        connection_config = ctx.connection_config
        remote_host = connection_config.host
        user = connection_config.user
        port = connection_config.port
        remote_workspace = ctx.remote_workspace_path
        remote_log = ctx.job_output_file_location
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)

        tail_conn = Connection(host=remote_host, user=user, port=port)

        def callback(item: str):
            self.output_tracker.got_output = True

        try:
            responsive_tail.responsive_tail(
                tail_conn,
                path=f"{remote_workspace}/{remote_log}",
                poll_interval=0.5,
                on_output_callback=callback,
            )
        except (KeyboardInterrupt, UnexpectedExit, SSHException, EOFError) as e:
            # Normal during remote process kill or connection close
            print(f"[tail] stopped: {type(e).__name__}")
        finally:
            tail_conn.close()
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

    def wait_for_log_sync(self, conn, path, retries=10, delay=1):
        for _ in range(retries):
            output = conn.run(f"stat {path}", hide=True, warn=True)
            if output.ok:
                return True
            else:
                print(output)
            time.sleep(delay)
        return False

    def run(self, ctx: Context):
        conn = ctx._connection
        job_id = ctx.job_id
        assert job_id is not None, (
            f"Job ID must be set in context before executing {type(self).__name__}."
        )

        tail_process_marker = f"slurm_executor_tail_{job_id}"

        t_tail = threading.Thread(
            target=self.tail_log, args=(ctx, tail_process_marker), daemon=True
        )
        t_tail.start()

        prev_state = None

        while True:
            res = conn.run(
                f"sacct -j {job_id} -X --format=JobID,State --noheader",
                hide=True,
                warn=True,
            )
            out = res.stdout.strip()
            if out:
                # take last token of last non-empty line as State
                state = out.splitlines()[-1].split()[-1]
            else:
                state = "UNKNOWN"

            if state != prev_state:
                print(f"[monitor] job {job_id} state changed: {prev_state} -> {state}")
            elif prev_state == "PENDING":
                print(".", end="", flush=True)
            if state in {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"}:
                # kill only the tail we started by matching the custom argv0
                # conn.run(f"pkill -f {tail_process_marker}", warn=False)
                t_tail.join()

                if self.output_tracker.got_output is False:
                    output_file_location = conn.run(
                        f"scontrol show job {job_id} | grep StdOut",
                        hide=True,
                    )

                    output_file_location = output_file_location.stdout.strip().split(
                        "="
                    )[1]
                    conn.run(
                        f"ls -lh {os.path.dirname(output_file_location)}", hide=True
                    )

                    if self.wait_for_log_sync(
                        conn,
                        output_file_location,
                    ):
                        conn.run(
                            f"cat {output_file_location}",
                            hide=None,
                        )
                        print(flush=True)

                if state != "COMPLETED":
                    raise Exception(f"Job {job_id} failed with state {state}.")

                break
            prev_state = state
            time.sleep(self.poll_interval_ms / 1000.0)

        return ctx
