from mpi4py import MPI  
from random import randint as rint
from math import sqrt
from statistics import mean,mode
from copy import deepcopy
from csv_tools import read_data_set,check_data_set_integrity
import sys
# how to represent a cluster
"""

cluster = centroids + set of instances

"""
CLUSTER = tuple[list,set[int]]
DATASET = list[list]


def divide_data_set(dataset_length:int,number_of_process:int):
    """
    This function return a list of tuple[start,end]
    """
    step = dataset_length // number_of_process
    start , end = 0,step
    positions = []
    while len(positions) < number_of_process:
        positions.append((start,end))
        start, end = end,end + step
    last_position = positions.pop()
    positions.append((last_position[0],dataset_length))
    return positions

def combaine(clusters:list[CLUSTER],to_append:list[CLUSTER]) -> list[CLUSTER]:
    cmb:dict[tuple:set[int]] = {tuple(cluster[0]):set(cluster[1]) for cluster in clusters}
    for cluster in to_append:
        cmb[tuple(cluster[0])] = cmb[tuple(cluster[0])].union((cluster[1]))
    return [(list(centroid),instances) for centroid,instances in cmb.items()]

def qst_continue(old_clusters:list[CLUSTER],new_clusters:list[CLUSTER]) -> bool:
    for old_cluster in old_clusters:
        for new_cluster in new_clusters:
            if old_cluster[0] == new_cluster[0] and old_cluster[1] != new_cluster[1]:
                return True
    return False

def re_compute_clusters(dataset:DATASET,clusters:list[CLUSTER]) -> list[CLUSTER]:
    new_clusters:list[CLUSTER] = []
    for cluster in clusters:
        new_cluster = [] 
        for attribute in range(len(cluster[0])):
            if isinstance(dataset[0][attribute],str):
                x = mode([dataset[_][attribute] for _ in cluster[1]])
            else:
                x = mean([dataset[_][attribute] for _ in cluster[1]])
            new_cluster.append(x)
        new_clusters.append((new_cluster,cluster[1]))
    return new_clusters
    

def master(comm:MPI.Intercomm,dataset:DATASET,k:int):
    NUMBER_OF_PROCESS = comm.Get_size()
    DATA_SET_SIZE = len(dataset)
    jobs = enumerate(divide_data_set(DATA_SET_SIZE,NUMBER_OF_PROCESS - 1),start=1)
    
    # sending the data set for each process 
    print(f"> List of jobs {list(deepcopy(jobs))}")
    for dest,(start,end) in jobs:
        print(f"> Sending the sub data set to {dest}")
        comm.send(obj=(dataset[start:end],start),dest=dest)
    
    x = set()
    while len(x) < k:
        x.add(rint(0,DATA_SET_SIZE-1))

    # picking a random clusters
    clusters : list[CLUSTER] = [(dataset[c],set()) for c in x]
    end = False
    # work
    while True:
        # sending clusters if is not the end
        to_send = 'END' if end else clusters
        for dest in range(1,NUMBER_OF_PROCESS):
            print(f"> Sending the message {to_send if to_send == 'END' else 'of the clusters'} to slave {dest}")
            comm.send(to_send,dest=dest)
        if to_send == 'END':
            break
        
        requests : list[MPI.Request] = []
        for source in range(1,NUMBER_OF_PROCESS):
            requests.append(comm.irecv(source=source))
        
        finished_process = NUMBER_OF_PROCESS - 1
        sub_result:list[CLUSTER]
        new_clusters:list[CLUSTER] = [(centroid.copy(),set()) for centroid,_ in clusters]

        print(f"> Waiting for {len(requests)} sub-results")
        while finished_process > 0:
            source,sub_result = MPI.Request.waitany(requests)
            print(f"> Recived sub results from slave {source}")
            new_clusters = combaine(new_clusters,sub_result)
            finished_process -= 1
        
        if not qst_continue(clusters,new_clusters):
            end = True
            continue
        #if input('> Continue Y/N ?').upper() != 'Y':break
        # recompute the centroids
        clusters = re_compute_clusters(dataset,new_clusters)
    print("> END MASTER")
    return clusters
        
        
def distance(x:list,y:list):
    d = 0.
    for i in range(min(len(x),len(y))):
        if isinstance(x[i],str):
            d += 1 if x[i] != y[i] else 0
        else:
            d += (x[i] - y[i])**2
        
    return sqrt(d)

def slave(comm:MPI.Intercomm):
    subdataset:DATASET
    offset:int
    print(f"> Slave {comm.Get_rank()} recived sub-data-set")
    subdataset,offset = comm.recv(source=0)

    # work
    while True:
        clusters: list[CLUSTER] = comm.recv(source=0)
        print(f"> Slave {comm.Get_rank()} recived the {'clusters' if clusters != 'END' else 'END'}")
        if clusters == 'END':
            break

        for instance in range(len(subdataset)):
            selected_cluster = min(clusters,key=lambda c:distance(c[0],subdataset[instance]))
            selected_cluster[1].add(instance + offset)
        print(f"> Slave {comm.Get_rank()} Finished appendning instances")
        comm.send(clusters,dest=0)
    print(f"> END SLAVE {comm.Get_rank()}")


if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    if comm.Get_rank() == 0:
        path = 'test.csv'
        k = 3
        dataset = read_data_set(path,True)
        if not check_data_set_integrity(dataset):
            print('The selected data set is not correct')
            sys.exit(-1)
        print(dataset[0])
        with open('a.out','w') as f:
            for line in master(comm,dataset,k):
                f.write(line.__str__()+'\n')
        print('The result are in the file a.out')
    else:
        slave(comm)
