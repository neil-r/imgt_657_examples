"""MPI Example spawn dynamic sub-processes

From Stack Overflow:
https://stackoverflow.com/questions/32257375/how-to-run-a-basic-mpi4py-code

This Python-script based MPI program demonstrates how a program spawn
sub-processes dynamically to compute PI. The sub-processes run the script
specified, "cpi.py".

To run this MPI program, execute:

> mpiexec -n 4 python driver_communicator.py

"""

from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == 0:
    data = {'a': 7, 'b': 3.14}
    comm.send(data, dest=1)
    print(f"Process with rank {rank} sent the following data: {data}")
elif rank == 1:
    data = comm.recv(source=0)
    # The following will receive from any other process, ignoring the source process's rank
    # data = comm.recv()
    print(f"Process with rank {rank} received the following data: {data}")
else:
    print(f"Process with rank {rank} doing nothing.")
