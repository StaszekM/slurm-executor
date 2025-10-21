from abc import ABC, abstractmethod

from slurm_executor.models.Context import Context


class Step(ABC):
    @abstractmethod
    def run(self, ctx: Context) -> Context:
        pass
