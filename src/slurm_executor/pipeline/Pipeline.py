import logging
from typing import Any, Callable, Generic, List, ParamSpec, Protocol, TypeVar, cast

from fabric import Connection

from slurm_executor.models.ConnectionConfig import ConnectionConfig
from slurm_executor.models.Context import Context
from slurm_executor.models.HookProtocol import Hook
from slurm_executor.models.Step import Step

P = ParamSpec("P")
T = TypeVar("T")


logger = logging.getLogger(__name__)


class RemoteCallable(Protocol[P, T]):
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T: ...

    local_run: Callable[P, T]


class Pipeline(Generic[P, T]):
    def __init__(
        self,
        steps: List[Step],
        connection_config: ConnectionConfig,
        hooks: List[Hook] | None = None,
    ):
        self.steps = steps
        self.connection_config = connection_config
        self.hooks = hooks or []

    def run(self, ctx: Context):
        exception_occurred = None
        try:
            for step in self.steps:
                logger.info(f"Running step: {step.__class__.__name__}")
                step.run(ctx)
            return ctx
        except BaseException as e:
            logger.error(f"Pipeline failed: {e}")
            exception_occurred = e
        finally:
            self._run_hooks(ctx, exception_occurred)

        if exception_occurred:
            raise exception_occurred

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

    def _run_hooks(self, ctx: Context, exception: BaseException | None) -> None:
        for hook in self.hooks:
            try:
                hook(ctx, exception)
            except Exception as hook_error:
                logger.error(f"Hook {hook.__class__.__name__} failed: {hook_error}")
                # Hooks should never break the pipeline
