from slurm_executor.models.Context import Context
from slurm_executor.models.Step import Step


class SubmitSbatchScript(Step):
    @property
    def provides(self) -> list[str]:
        return ["job_id", "job_output_file_location"]

    @property
    def requires(self) -> list[str]:
        return ["remote_sbatch_path", "remote_workspace_path"]

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
