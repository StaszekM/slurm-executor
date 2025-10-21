from pydantic import BaseModel


class ConnectionConfig(BaseModel):
    host: str
    user: str
    port: int = 22
