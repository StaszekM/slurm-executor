from abc import ABC, abstractmethod
from typing import Optional

from slurm_executor import Context


class BaseHook(ABC):
    """Abstract base class for hooks"""

    @abstractmethod
    def execute(self, ctx: Context, exception: Optional[BaseException] = None) -> None:
        """Hook implementation"""
        pass

    def __call__(self, ctx: Context, exception: Optional[BaseException] = None) -> None:
        self.execute(ctx, exception)
