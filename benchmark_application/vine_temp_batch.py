import ndcctools.taskvine as vine
from tqdm import tqdm

num_workers = 20
num_cores = 12
intermediate_data_size_mb = 50
intermediate_data_size = int(intermediate_data_size_mb*1e6)

m = vine.Manager(port=9285, name="vine_temp_benchmark")
f = vine.Factory("condor", manager_name="vine_temp_benchmark")
f.min_workers=num_workers
f.max_workers=num_workers
f.workers_per_cycle = 1000
f.disk=50000
f.cores=num_cores

m.tune('task-groups', 0)
m.tune('wait-for-workers', num_workers)
m.tune('prefer-dispatch', 1)

num_tasks = 20
num_intermediate_stages = 100

with f:
    for i in range(num_tasks):
        temp_initial = m.declare_temp()
        sequence_begin = vine.Task(f"dd if=/dev/zero of=file1-{i} bs={intermediate_data_size} count=1")
        sequence_begin.add_output(temp_initial, f"file1-{i}")
        prev_output = None
        m.submit(sequence_begin)
        for j in range(num_intermediate_stages-1):
            temp_intermediate = m.declare_temp()
            if prev_output is None:
                prev_output = temp_intermediate
                t = vine.Task(f"dd if=file1-{i} of=file{j+1}-{i}-{j} bs={intermediate_data_size} count=1")
                t.add_input(temp_initial, f"file1-{i}")
                t.add_output(temp_intermediate, f"file{j+1}-{i}-{j}")
                m.submit(t)
            else:
                tmp = vine.Task(f"dd if=file{j}-{i}-{j-1} of=file{j+1}-{i}-{j} bs={intermediate_data_size} count=1")
                tmp.add_input(prev_output, f"file{j}-{i}-{j-1}")
                prev_output = m.declare_temp()
                tmp.add_output(prev_output, f"file{j+1}-{i}-{j}")
                m.submit(tmp)
    prog = tqdm(total=num_tasks+num_tasks*(num_intermediate_stages-1))
    while not m.empty():
        t = m.wait(10)
        if t:
            prog.update(1)
    prog.close()


