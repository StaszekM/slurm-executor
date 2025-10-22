from typing import Optional, Protocol

from slurm_executor import Context


class Hook(Protocol):
    """Protocol for pipeline hooks"""

    def __call__(self, ctx: Context, exception: Optional[BaseException] = None) -> None:
        """
        Execute hook.

        Args:
            ctx: Pipeline context
            exception: None if pipeline succeeded, Exception if it failed
        """
        ...
