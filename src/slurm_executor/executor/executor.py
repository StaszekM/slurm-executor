import inspect
import json
import os
import pathlib
import tempfile
import time as libtime

from fabric import Connection


def slurm_task(
    partition="short", time="00:10:00", remote=None, workdir="~/remote_jobs"
):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if remote is None:
                # run locally for testing
                print(f"[local] Running {func.__name__}")
                return func(*args, **kwargs)

            # --- serialize call ---
            func_name = func.__name__
            func_src = inspect.getsource(func)
            call_data = {
                "func_name": func_name,
                "args": args,
                "kwargs": kwargs,
                "func_src": func_src,
            }

            tmp = tempfile.TemporaryDirectory()
            local_job_dir = pathlib.Path(tmp.name)
            call_file = local_job_dir / "call.json"
            call_file.write_text(json.dumps(call_data))

            # --- rsync codebase to remote ---
            remote_path = f"{workdir}/{func_name}_{int(libtime.time())}"
            print(f"[remote] Syncing to {remote}:{remote_path}")
            os.system(f'rsync -az --exclude=".git" ./ {remote}:{remote_path}/')

            # --- submit job ---
            script = f"""#!/bin/bash
#SBATCH --partition={partition}
#SBATCH --time={time}
cd {remote_path}
python3 - <<'EOF'
import json
import sys
call = json.load(open('call.json'))
exec(call["func_src"])
globals()[call["func_name"]](*call["args"], **call["kwargs"])
EOF
"""
            local_script = local_job_dir / "job.sh"
            local_script.write_text(script)

            os.system(f"rsync {local_script} {remote}:{remote_path}/job.sh")
            conn = Connection(remote)
            result = conn.run(f"cd {remote_path} && sbatch job.sh", hide=True)
            job_id = result.stdout.strip().split()[-1]
            print(f"[remote] Submitted job {job_id}")

            # --- simple polling until job completes ---
            while True:
                out = conn.run(
                    f"sacct -j {job_id} --format=State --noheader", hide=True
                ).stdout.strip()
                if (
                    out.startswith("COMPLETED")
                    or out.startswith("FAILED")
                    or out.startswith("CANCELLED")
                ):
                    print(f"[remote] Job {job_id} finished: {out}")
                    break
                libtime.sleep(5)
            return None

        return wrapper

    return decorator
