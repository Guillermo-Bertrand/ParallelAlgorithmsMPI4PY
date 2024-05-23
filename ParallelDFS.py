from mpi4py import MPI
from collections import defaultdict

def DFS(graph, start, visited):
    stack = [start]
    local_result = []

    while stack:
        vertex = stack.pop()
        if vertex not in visited:
            visited.add(vertex)
            local_result.append(vertex)
            stack.extend(reversed(graph[vertex]))

    return local_result

graph = defaultdict(list)
graph[0] = [1, 2]
graph[1] = [0, 3]
graph[2] = [0, 4]
graph[3] = [1, 5]
graph[4] = [2, 5]
graph[5] = [3, 4]

if __name__ == "__main__":
  comm = MPI.COMM_WORLD
  rank = comm.Get_rank()
  size = comm.Get_size()

  all_nodes = list(graph.keys())
  chunk_size = len(all_nodes) // size
  local_nodes = all_nodes[rank * len(all_nodes): (rank + 1) * len(all_nodes)]

  visited = set()
  local_result = []

  for node in local_nodes:
    if node not in visited:
      local_result.extend(DFS(graph, node, visited))

  gathered_results = comm.gather(local_result, root=0)

  if rank == 0:
    final_result = []
    for result in gathered_results:
      final_result.extend(result)
    print("Parallel DFS traversing: ", final_result)













      