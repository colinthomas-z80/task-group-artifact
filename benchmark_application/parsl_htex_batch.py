import parsl
from parsl import bash_app, bash_app
from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.providers import LocalProvider
from parsl.executors import HighThroughputExecutor
from parsl.executors.taskvine import TaskVineExecutor
from parsl.executors.taskvine import TaskVineFactoryConfig
from parsl.executors.taskvine import TaskVineManagerConfig
from parsl.executors.taskvine.taskvine_staging_provider import TaskVineStaging
from parsl.data_provider.files import File
from concurrent.futures import as_completed

from parsl.executors import HighThroughputExecutor
from parsl.providers import GridEngineProvider
from parsl.providers import CondorProvider
from parsl.addresses import address_by_interface

from tqdm import tqdm
import time

num_workers = 20
num_cores = 24
intermediate_data_size_mb = 200
intermediate_data_size = int(intermediate_data_size_mb*1e6)

#HTEX config
config = Config(
    executors=[HighThroughputExecutor(
        provider=CondorProvider(
            nodes_per_block=1,
            init_blocks=num_workers,
            max_blocks=num_workers,
            worker_init="unset PYTHONPATH; source /project01/ndcms/cthoma26/miniconda3/etc/profile.d/conda.sh; conda activate /project01/ndcms/cthoma26/miniconda3/envs/parsldockenv;",
        #    scheduler_options="#$ -pe smp 12\n#$ -q long",
            scheduler_options=f"request_cpus={num_cores}\nrequest_memory=4096",
        ),
    max_workers_per_node=1,
    )],
)

#Local config
#config = Config(
#    executors=[HighThroughputExecutor(),
#    ],
#)

num_tasks = 20
num_intermediate_stages = 100

# the initial function
@bash_app
def f1(intermediate_data_size, outputs = [], stdout='f1_stdout'):
    return f"dd if=/dev/zero of={outputs[0]} bs={intermediate_data_size} count=1"    

# intermediate function
@bash_app
def f2(intermediate_data_size, inputs = [], outputs = [], stdout='f2_stdout', stderr ='f2_stderr'):
    return f"dd if={inputs[0]} of={outputs[0]} bs={intermediate_data_size} count=1"

# final function
@bash_app
def f3(inputs = []):
    return f"cat {inputs[0]}"

with parsl.load(config) as conf:
    time.sleep(60)
    list_finals = []
    for i in range(num_tasks):
        temp_initial = File(f"/project01/ndcms/cthoma26/parsl-sequential-temps/scratch_space/file1-{i}")
        sequence_begin = f1(intermediate_data_size, outputs=[temp_initial])
        prev_output = None
        for j in range(num_intermediate_stages):
            temp_intermediate = File(f"/project01/ndcms/cthoma26/parsl-sequential-temps/scratch_space/file{j+1}-{i}-{j}")
            if prev_output is None:
                prev_output = f2(intermediate_data_size, inputs=[sequence_begin.outputs[0]], outputs=[temp_intermediate])
            else:
                tmp = f2(intermediate_data_size, inputs=[prev_output.outputs[0]], outputs=[temp_intermediate])
                prev_output = tmp
        list_finals.append(f3(inputs=[prev_output.outputs[0]]))


    prog = tqdm(total=len(list_finals))
    while len(list_finals) > 0:
        future = next(as_completed(list_finals))
        list_finals.remove(future)
        prog.update(1)
    prog.close()


