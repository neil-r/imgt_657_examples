"""Python script demonstrating Processes with un-shared memory

This script starts sub-processes; each sub-process appends a random number to a
variable that is part of the process's copied memory. The main process
waits for the child processes to complete and then prints the changes to the 
variable. Since the sub-processes work on a copy of the variable, the main
process does not see the changes performed to the variable in the other
process's copied memory.

To run this script, execute:

> python driver_no_shared_memory.py

"""

from time import sleep, perf_counter
from multiprocessing import Process, Queue
import random

def task():
    print('Starting a task...')
    sleep(3 * random.random())
    x.append(random.random())
    print('done')

x = []

start_time = perf_counter()

if __name__ == '__main__':
    # create two new processes
    t1 = Process(target=task)
    t2 = Process(target=task)

    # start the processes
    t1.start()
    t2.start()

    # wait for the processes to complete
    t1.join()
    t2.join()

    end_time = perf_counter()

    print(f'It took {end_time- start_time: 0.2f} second(s) to complete.')
    print(f"Notice that no changes made to x = {x}, from the process tasks using non-shared memory")
