import pathlib
import tempfile
from pathlib import Path
from typing import Optional

import cloudpickle

from slurm_executor.models.Context import Context
from slurm_executor.models.SerializableCallData import SerializableCallData
from slurm_executor.pipeline.Step import Step


class SerializeCall(Step):
    def __init__(self, serialize_to: Optional[Path] = None) -> None:
        super().__init__()

        self.serialize_to = serialize_to

    def run(self, ctx: Context):
        func = ctx.function
        args = ctx.args
        kwargs = ctx.kwargs

        call_data = SerializableCallData(
            func=func,
            args=args,
            kwargs=kwargs,
        )

        if self.serialize_to is not None:
            with open(self.serialize_to, "wb") as f:
                cloudpickle.dump(call_data, f)
            ctx.serialized_call_path = self.serialize_to
        else:
            tmp = tempfile.TemporaryDirectory(delete=False)
            local_job_dir = pathlib.Path(tmp.name)
            call_file = local_job_dir / "call.pkl"
            with open(call_file, "wb") as f:
                cloudpickle.dump(call_data, f)
            ctx.serialized_call_path = call_file
        return ctx
