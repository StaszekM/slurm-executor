from pathlib import Path
from typing import Any, Callable

import cloudpickle

from slurm_executor.models.SerializableCallData import SerializableCallData


class CloudpickleExecutor:
    def __init__(self, serialize_to: str | Path, deserialize_from: str):
        self.serialize_to = serialize_to
        self.deserialize_from = deserialize_from

    def serialize_call(
        self, func: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> None:
        call_data = SerializableCallData(func=func, args=args, kwargs=kwargs)
        with open(self.serialize_to, "wb") as f:
            cloudpickle.dump(call_data, f)  # pyright: ignore[reportUnknownMemberType]

    def run(self) -> Any:
        call_data = self._deserialize_call()
        return call_data.func(*call_data.args, **call_data.kwargs)

    def _deserialize_call(self) -> SerializableCallData:
        with open(self.deserialize_from, "rb") as f:
            call_data = cloudpickle.load(f)
        return call_data
