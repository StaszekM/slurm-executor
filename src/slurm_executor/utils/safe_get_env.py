"""Utility for safely retrieving environment variables."""

import os


def safe_get_env(key: str, description: str = "") -> str:
    """
    Get an environment variable or raise an error if it doesn't exist.

    This function ensures that required environment variables are present
    and avoids silently using default values that may lead to unexpected behavior.

    Args:
        key: The environment variable name to retrieve
        description: Optional description of what the variable is used for

    Returns:
        The value of the environment variable

    Raises:
        ValueError: If the environment variable is not set

    Example:
        >>> slurm_remote = safe_get_env("SLURM_REMOTE", "SLURM cluster hostname")
    """
    value = os.getenv(key)
    if value is None:
        error_msg = f"Required environment variable '{key}' is not set."
        if description:
            error_msg += f" {description}"
        raise ValueError(error_msg)
    return value
