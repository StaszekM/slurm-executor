class FailedSbatchError(Exception):
    """Exception raised when sbatch command fails."""

    def __init__(self, message: str):
        super().__init__(message)
