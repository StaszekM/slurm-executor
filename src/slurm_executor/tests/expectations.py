import re
from typing import List, Optional

from slurm_executor.tests.ssh_mock import MockSSHConnection


def expect_slurm_job_submission(
    mock_conn: MockSSHConnection,
    job_id: str = "12345",
    workspace_path: str = "/home/user/remote_job",
) -> MockSSHConnection:
    """
    Add common SLURM job submission expectations.

    Args:
        mock_conn: MockSSHConnection to configure
        job_id: Job ID to return from sbatch
        workspace_path: Remote workspace path

    Returns:
        The configured mock connection
    """
    return mock_conn.expect_command(
        rf"cd {re.escape(workspace_path)} && sbatch --parsable --output=.*",
        stdout=f"{job_id};",
        is_regex=True,
    )


def expect_slurm_job_monitoring(
    mock_conn: MockSSHConnection,
    job_id: str = "12345",
    final_state: str = "COMPLETED",
    intermediate_states: Optional[List[str]] = None,
    stdout_path: str = "/home/user/remote_job/job.out",
) -> MockSSHConnection:
    """
    Add common SLURM job monitoring expectations.

    Args:
        mock_conn: MockSSHConnection to configure
        job_id: Job ID to monitor
        final_state: Final job state (COMPLETED, FAILED, etc.)
        intermediate_states: List of intermediate states before final state
        stdout_path: Path to job output file

    Returns:
        The configured mock connection
    """
    # Job state monitoring
    if intermediate_states:
        for state in intermediate_states:
            mock_conn.expect_command(
                f"sacct -j {job_id} -X --format=JobID,State --noheader",
                stdout=f"{job_id} {state}",
            )

    mock_conn.expect_command(
        f"sacct -j {job_id} -X --format=JobID,State --noheader",
        stdout=f"{job_id} {final_state}",
    )

    # Stdout path retrieval
    mock_conn.expect_command(
        f"scontrol show job {job_id} | grep StdOut", stdout=f"StdOut={stdout_path}"
    )

    # File availability check
    workspace_dir = stdout_path.rsplit("/", 1)[0]
    mock_conn.expect_command(
        f"ls {workspace_dir} > /dev/null", stdout=""
    ).expect_command(
        f"test -f {stdout_path} && echo 'exists' || echo 'not_exists'", stdout="exists"
    )

    # Job output reading
    mock_conn.expect_command(f"cat {stdout_path}", stdout="Job completed successfully!")

    return mock_conn


def expect_rsync_operations(
    mock_conn: MockSSHConnection, remote_workspace: str = "/home/user/remote_job"
) -> MockSSHConnection:
    """
    Add common rsync operation expectations.

    Args:
        mock_conn: MockSSHConnection to configure
        remote_workspace: Remote workspace path

    Returns:
        The configured mock connection
    """
    return mock_conn.expect_command(
        f"mkdir -p {remote_workspace}", stdout=""
    ).expect_command(
        r"rsync -e 'ssh -p \d+' --delete --info=progress2 -az.*",
        stdout="rsync completed",
        is_regex=True,
    )
