import logging
from typing import Optional

from slurm_executor import Context
from slurm_executor.hooks.BaseHook import BaseHook

logger = logging.getLogger(__name__)


class CancelJobHook(BaseHook):
    """Cancel SLURM job on pipeline failure"""

    def execute(self, ctx: Context, exception: Optional[Exception] = None) -> None:
        if exception is None:
            return  # Only run on failure

        if hasattr(ctx, "job_id") and ctx.job_id:
            conn = ctx._connection
            cancel_command = f"scancel {ctx.job_id}"
            conn.run(cancel_command)
            logger.info(f"Cancelled SLURM job {ctx.job_id}")
