import inspect
import pathlib
import tempfile
from typing import Set

import jinja2
from jinja2 import Environment, meta

from slurm_executor.models.Context import Context
from slurm_executor.models.Step import Step
from slurm_executor.utils.append_path_slash_if_missing import (
    append_path_slash_if_missing,
)
from slurm_executor.utils.compose_rsync_command import compose_rsync_command


def get_template_undeclared_variables(template_content_string: str) -> Set[str]:
    """Get undeclared variables in a Jinja2 template. Undeclared variables are those
    that are used in the template but not defined within it.

    Parameters
    ----------
    template_content_string : str
        The content of the Jinja2 template as a string.

    Returns
    -------
    Set[str]
        A set of undeclared variable names found in the template.
    """
    env = Environment()
    parsed_content = env.parse(template_content_string)

    undeclared = meta.find_undeclared_variables(parsed_content)
    return undeclared


class SendSbatchScript(Step):
    @property
    def provides(self) -> list[str]:
        return ["remote_sbatch_path"]

    @property
    def requires(self) -> list[str]:
        return ["remote_workspace_path", "remote_call_path"]

    def __init__(
        self, partition: str, time: str, sbatch_script_template_location: str
    ) -> None:
        super().__init__()
        self.partition = partition
        self.time = time

        if not pathlib.Path(sbatch_script_template_location).is_file():
            raise FileNotFoundError(
                f"SBATCH script template file not found at \
{sbatch_script_template_location}."
            )

        with open(sbatch_script_template_location, "r") as f:
            sbatch_template_file_contents = f.read()

        sig = inspect.signature(SendSbatchScript.compose_sbatch_script)
        required_kwargs = [
            name
            for name, param in sig.parameters.items()
            if param.default is param.empty and param.kind == param.KEYWORD_ONLY
        ]
        self.required_variables = set(required_kwargs)

        self._validate_template_variables(sbatch_template_file_contents)

        self.sbatch_template = jinja2.Template(sbatch_template_file_contents)

    def _validate_template_variables(self, sbatch_template_file_contents: str) -> None:
        undeclared_variables = get_template_undeclared_variables(
            sbatch_template_file_contents
        )

        missing_variables = self.required_variables - undeclared_variables
        unexpected_variables = undeclared_variables - self.required_variables

        if unexpected_variables and missing_variables:
            raise ValueError(
                f"The SBATCH script template is missing required variables: \
{', '.join(missing_variables)} and contains unexpected variables: \
{', '.join(unexpected_variables)}."
            )

        if missing_variables:
            raise ValueError(
                f"The SBATCH script template is missing required variables: \
{', '.join(missing_variables)}."
            )

        if unexpected_variables:
            raise ValueError(
                f"The SBATCH script template contains unexpected variables: \
{', '.join(unexpected_variables)}."
            )

    def run(self, ctx: Context):
        conn = ctx._connection
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
        composed_file_contents = self.compose_sbatch_script(
            partition=self.partition,
            time=self.time,
            workspace_location=workspace_location,
            remote_call_location=remote_call_location,
        )
        with tempfile.TemporaryDirectory() as tmp:
            job_script_name = "job.sh"
            local_job_dir = pathlib.Path(tmp)
            local_script = local_job_dir / job_script_name
            with open(local_script, "w") as f:
                f.write(composed_file_contents)

            conn = ctx._connection

            remote_sbatch_location = (
                append_path_slash_if_missing(workspace_location) + job_script_name
            )

            identity_file_path = ctx.connection_config.connect_kwargs.get(
                "key_filename"
            )

            conn.local(  # pyright: ignore[reportUnknownMemberType]
                compose_rsync_command(
                    port=ctx.connection_config.port,
                    user=ctx.connection_config.user,
                    host=ctx.connection_config.host,
                    local_root=str(local_script),
                    remote_root=remote_sbatch_location,
                    exclusion_file=None,
                    direction="to_remote",
                    identity_file_path=identity_file_path,
                ),
                pty=False,
            )

            ctx.remote_sbatch_path = remote_sbatch_location

        return ctx

    def compose_sbatch_script(
        self,
        *,
        partition: str,
        time: str,
        workspace_location: str,
        remote_call_location: str,
    ) -> str:
        return self.sbatch_template.render(
            partition=partition,
            time=time,
            workspace_location=workspace_location,
            remote_call_location=remote_call_location,
        )
