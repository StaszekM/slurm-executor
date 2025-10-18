from pathlib import Path
from typing import Any, Callable

import cloudpickle
import jinja2

from slurm_executor.models.SerializableCallData import SerializableCallData

with open("src/slurm_executor/executor/sbatch_script.jinja") as f:
    SBATCH_TEMPLATE = jinja2.Template(f.read())


def compose_sbatch_script(partition: str, time: str, workspace_location: str) -> str:
    return SBATCH_TEMPLATE.render(
        partition=partition, time=time, workspace_location=workspace_location
    )


class CloudpickleExecutor:
    def __init__(
        self,
        serialize_to: str | Path,
        deserialize_from: str,
        partition: str,
        time: str,
        workspace_location: str,
    ):
        self.serialize_to = serialize_to
        self.deserialize_from = deserialize_from

        self.partition = partition
        self.time = time
        self.workspace_location = workspace_location

    def serialize_call(
        self, func: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> None:
        call_data = SerializableCallData(
            func=func,
            args=args,
            kwargs=kwargs,
        )
        with open(self.serialize_to, "wb") as f:
            cloudpickle.dump(call_data, f)  # pyright: ignore[reportUnknownMemberType]

    def serialize_sbatch_script(self, location: Path) -> None:
        composed_file_contents = compose_sbatch_script(
            partition=self.partition,
            time=self.time,
            workspace_location=self.workspace_location,
        )

        location.write_text(composed_file_contents)

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
