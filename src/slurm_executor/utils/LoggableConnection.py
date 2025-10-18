import logging
from typing import Any, Optional

from fabric import Connection
from invoke.runners import Result

logger = logging.getLogger(__name__)


class LoggableConnection(Connection):
    def run(self, command: str, *args: Any, **kwargs: Any) -> Optional[Result]:
        logger.info(f"Running command: {command}")
        return super().run(command, *args, **kwargs)

    def local(self, command: str, *args: Any, **kwargs: Any) -> Optional[Result]:
        logger.info(f"Running local command: {command}")
        return super().local(command, *args, **kwargs)
