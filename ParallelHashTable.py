from mpi4py import MPI
import hashlib

class ParallelHashTable:
    def __init__(self, size_per_process):
        self.size_per_process = size_per_process
        self.table = [{} for _ in range(size_per_process)]
    
    def hash_function(self, key):
        hashed_key = hashlib.sha1(str(key).encode()).hexdigest()
        return int(hashed_key, 16) % self.size_per_process
    
    def insert(self, key, value):
        index = self.hash_function(key)
        self.table[index][key] = value
    
    def get(self, key):
        index = self.hash_function(key)
        return self.table[index].get(key, None)

if __name__ == "__main__":
  comm = MPI.COMM_WORLD
  rank = comm.Get_rank()
  size = comm.Get_size()

  print(size)

  size_per_process = 10  # Aux size per process.
  hashTable = ParallelHashTable(size_per_process)

  if rank == 0:
    #If main process, insert some values into hashtable.
    hashTable.insert(5, "Value 5")
    hashTable.insert(15, "Value 15")
    hashTable.insert(25, "Value 25")
    hashTable.insert(45, "Value 45")
    hashTable.insert(55, "Value 55")
    hashTable.insert(65, "Value 65")
    # And share hash table with all processes.
    for i in range(1, size):
      comm.send(hashTable.table, dest=i, tag=i)
  else:
  # If not main process, just receive hashtable from main.
    hashTable.table = comm.recv(source=0, tag=rank)
  # And perform some searching and insertions.
  if rank == 0:
    print("Adding and searching elements...")
    hashTable.insert(35, "Value 35")
    print("Value for key 35:", hashTable.get(35))
    print("Value for key 15:", hashTable.get(15))
    print("Value for key 65:", hashTable.get(65))
    print("Value for key 25:", hashTable.get(25))
    print("Value for key 1:", hashTable.get(1))
  else:
    print(f"Process {rank} received hash table.")
