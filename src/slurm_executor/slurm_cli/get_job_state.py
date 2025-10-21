import fabric


def get_job_state(conn: fabric.Connection, job_id: str | int) -> str:
    res = conn.run(
        f"sacct -j {job_id} -X --format=JobID,State --noheader",
        hide=True,
        warn=True,
    )
    out = res.stdout.strip()
    if out:
        # take last token of last non-empty line as State
        state = out.splitlines()[-1].split()[-1]
    else:
        state = "UNKNOWN"
    return state
