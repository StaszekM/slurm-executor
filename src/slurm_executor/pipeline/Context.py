from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import fabric
from pydantic import BaseModel, PrivateAttr

from slurm_executor.pipeline.Pipeline import ConnectionConfig


class Context(BaseModel):
    function: Callable
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    _connection: fabric.Connection = PrivateAttr()
    connection_config: ConnectionConfig

    serialized_call_path: Optional[Path] = None
    serialized_sbatch_path: Optional[Path] = None
    remote_workspace_path: Optional[str] = None
    remote_call_path: Optional[str] = None
    remote_sbatch_path: Optional[str] = None
    job_output_file_location: Optional[str] = None
    job_id: Optional[str] = None

    def attach_connection(self, conn: fabric.Connection):
        self._connection = conn
