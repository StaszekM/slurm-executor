from typing import Any

import cloudpickle

from slurm_executor.models.SerializableCallData import SerializableCallData


class CloudpickleExecutor:
    def __init__(
        self,
        deserialize_from: str,
    ):
        self.deserialize_from = deserialize_from

    def run(self) -> Any:
        call_data = self._deserialize_call()
        return call_data.func(*call_data.args, **call_data.kwargs)

    def _deserialize_call(self) -> SerializableCallData:
        with open(self.deserialize_from, "rb") as f:
            call_data = cloudpickle.load(f)
        assert isinstance(call_data, SerializableCallData), (
            "Deserialized data is not of type SerializableCallData"
        )
        return call_data
