import ray
import time
import random

# Start Ray. If you're connecting to an existing cluster, you would use
# ray.init(address=<cluster-address>) instead.

ray.init()


# A regular Python function.
def my_function():
    return 1

# By adding the `@ray.remote` decorator, a regular Python function
# becomes a Ray remote function.
@ray.remote
def my_function():
    return 1

# To invoke this remote function, use the `remote` method.
# This will immediately return an object ref (a future) and then create
# a task that will be executed on a worker process.
obj_ref = my_function.remote()

@ray.remote
def slow_function(wait_time, msg):
    time.sleep(wait_time)
    print(f"Waited {wait_time} seconds to print out {msg}")
    return 1

# Invocations of Ray remote functions happen in parallel.
# All computation is performed in the background, driven by Ray's internal event loop.
obj_refs=[]
for i in range(4):
    # This doesn't block.
   obj_ref = slow_function.remote(random.random(), f"Hello for the {i}th time.")
   obj_refs.append(obj_ref)

assert ray.get(obj_refs) == [1, 1, 1, 1]