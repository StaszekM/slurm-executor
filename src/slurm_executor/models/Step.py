from abc import ABC, abstractmethod

from slurm_executor.models.Context import Context


class Step(ABC):
    @property
    @abstractmethod
    def provides(self) -> list[str]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def requires(self) -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def run(self, ctx: Context) -> Context:
        raise NotImplementedError()
