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

from tqdm import tqdm

num_workers = 20
num_cores = 12
intermediate_data_size_mb = 50
intermediate_data_size = int(intermediate_data_size_mb*1e6)


config = Config(
        executors=[
            TaskVineExecutor(
                manager_config=TaskVineManagerConfig(tune_parameters={'task-groups':1, 'wait-for-workers':num_workers}, port=9285, project_name="tv_parsl_temps"),
                #manager_config=TaskVineManagerConfig(enable_peer_transfers=False, tune_parameters={'wait-for-workers':num_workers}, port=9129, project_name="tv_parsl_temps"),
	            factory_config=TaskVineFactoryConfig(min_workers=num_workers, max_workers=num_workers, batch_type="condor", workers_per_cycle=200, cores=num_cores, worker_timeout=1500, disk=60000, memory=4096,batch_options="error=condor.err\noutput=condor.output"),
                storage_access=[TaskVineStaging()],
   #             worker_launch_method='manual',
            ),]
        )   

num_tasks = 20
num_intermediate_stages = 10

# the initial function
@bash_app
def f1(intermediate_data_size, outputs = [], parsl_resource_specification = {'disk':10000, 'cores':num_cores, 'memory':1024,}):
    return f"dd if=/dev/zero of={outputs[0]} bs={intermediate_data_size} count=1"    

# intermediate function
@bash_app
def f2(intermediate_data_size, inputs = [], outputs = [], parsl_resource_specification = {'disk':10000, 'cores':num_cores, 'memory':1024,}):
    return f"dd if={inputs[0]} of={outputs[0]} bs={intermediate_data_size} count=1"

# final function
@bash_app
def f3(inputs = [], parsl_resource_specification = {'disk':10000, 'cores':num_cores, 'memory':1024,}):
    return f"cat {inputs[0]}"

with parsl.load(config) as conf:
    
    list_finals = []
    for i in range(num_tasks):
        temp_initial = File(f"taskvinetemp://file1-{i}")
        sequence_begin = f1(intermediate_data_size, outputs=[temp_initial])
        prev_output = None
        for j in range(num_intermediate_stages):
            temp_intermediate = File(f"taskvinetemp://file{j+1}-{i}-{j}")
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


