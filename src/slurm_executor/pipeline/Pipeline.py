import logging
from typing import Any, Callable, Generic, List, ParamSpec, Protocol, TypeVar, cast

from fabric import Connection

from slurm_executor.models.ConnectionConfig import ConnectionConfig
from slurm_executor.models.Context import Context
from slurm_executor.models.Step import Step

P = ParamSpec("P")
T = TypeVar("T")


logger = logging.getLogger(__name__)


class RemoteCallable(Protocol[P, T]):
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T: ...

    local_run: Callable[P, T]


class Pipeline(Generic[P, T]):
    def __init__(self, steps: List[Step], connection_config: ConnectionConfig):
        self.steps = steps
        self.connection_config = connection_config

    def run(self, ctx: Context):
        for step in self.steps:
            logger.info(f"Running step: {step.__class__.__name__}")
            step.run(ctx)
        return ctx

    def verify(self) -> None:
        provided: set[str] = set()
        for step in self.steps:
            for req in step.requires:
                if req not in provided:
                    raise ValueError(
                        f"Step {step.__class__.__name__} requires '{req}' which is not provided by any previous step."  # noqa: E501
                    )
            for prov in step.provides:
                provided.add(prov)

    def remote_run(self, func: Callable[P, T]) -> RemoteCallable[P, T]:
        self.verify()

        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            with Connection(**self.connection_config.model_dump()) as conn:
                ctx = Context(
                    function=func,
                    args=args,
                    kwargs=kwargs,
                    connection_config=self.connection_config,
                )
                ctx.attach_connection(conn)

                return self.run(ctx)

        wrapper.local_run = func

        return cast(RemoteCallable[P, T], wrapper)
