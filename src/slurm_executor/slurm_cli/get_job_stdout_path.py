import fabric


def get_job_stdout_path(conn: fabric.Connection, job_id: str | int) -> str:
    return (
        conn.run(
            f"scontrol show job {job_id} | grep StdOut",
            hide=True,
        )
        .stdout.replace("StdOut=", "")
        .strip()
    )
