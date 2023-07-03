"""Python script demonstrating Threads shared memory

This script creates threads; each thread appends a random number to a variable
that is part of the process's shared memory. The main thread waits for
the child threads to complete and then prints the changes to the shared
variable (in shared memory).

To run this script, execute:

> python driver_shared_memory_threads.py

"""

from time import sleep, perf_counter
from threading import Thread
import random

def task():
    print('Starting a task...')
    sleep(3 * random.random())
    x.append(random.random())
    print('done')

x = []

start_time = perf_counter()

# create two new threads
t1 = Thread(target=task)
t2 = Thread(target=task)

# start the threads
t1.start()
t2.start()

# wait for the threads to complete
t1.join()
t2.join()

end_time = perf_counter()

print(f'It took {end_time- start_time: 0.2f} second(s) to complete.')
print(f"Notice the changes made to x = {x}, from the thread tasks using shared memory")
