from typing import Any, Callable

import invoke

from slurm_executor import Context, Step


class ExecuteCommand(Step):
    @property
    def provides(self) -> list[str]:
        return []

    @property
    def requires(self) -> list[str]:
        return []

    def __init__(
        self,
        remote_command: str,
        on_success: Callable[[Any], None] | None = None,
        on_failure: Callable[[Any], None] | None = None,
    ) -> None:
        super().__init__()
        self.remote_command = remote_command
        self.on_success = on_success
        self.on_failure = on_failure

    def run(self, ctx: Context):
        conn = ctx._connection

        result: invoke.runners.Result = conn.run(self.remote_command)

        if result.ok:
            if self.on_success is not None:
                self.on_success(result)
        else:
            if self.on_failure is not None:
                self.on_failure(result)

        return ctx
