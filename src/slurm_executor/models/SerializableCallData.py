from typing import Any

from pydantic import BaseModel


class SerializableCallData(BaseModel):
    func: Any
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
