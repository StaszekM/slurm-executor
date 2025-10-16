from slurm_executor import slurm_task


@slurm_task(remote="hpc.cluster.local", time="00:05:00", partition="cpu")
def write_to_standard_output(text):
    print(f"Writing to standard output: {text}")


if __name__ == "__main__":
    write_to_standard_output("Hello, world!")
