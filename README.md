
> [!WARNING]  
>This project is under heavy development and is not ready for any promised use case yet!

# A library for transparent execution of heavy Python jobs on SLURM!

Imagine being able to offload **any** Python function to a HPC cluster with just few lines of code!

No more logging into a cluster, git pulling, troubleshooting and other headaches.

Just one decorator away:

```python

from slurm_executor import slurm_task
from your_code import heavy_data_load, heavy_data_process, heavy_data_save
import os

@slurm_task(partition="gpu", time="00:30:00", remote="hpc.example.com")
def heavy_step(input_file, output_file):
    data = heavy_data_load()
    processed = heavy_data_process(data)
    heavy_data_save(remote=os.getenv('s3://my_bucket'))

```

Unleash the power of completely transparent offloading.

You decide what needs to be executed remotely, how many resources need to be allocated, and many more!