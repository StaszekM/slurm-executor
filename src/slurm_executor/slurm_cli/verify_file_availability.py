import fabric


def verify_file_availability(
    conn: fabric.Connection, file_directory: str, file: str
) -> bool:
    # extra ls to dump any network filesystem caches (Lustre etc.)
    conn.run(f"ls {file_directory} > /dev/null", hide=True)
    command = f"test -f {file} && echo 'exists' || echo 'not_exists'"
    file_check = conn.run(
        command,
        hide=True,
    )
    if file_check.stdout.strip() == "exists":
        return True
    return False
