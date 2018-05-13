# latency.py
# Benchmark the memcached server latency across a Zipfian distribution with
# concurrent clients.

import os.path
import time
import sys
import numpy.random
import hashlib
import multiprocessing

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
if not BASE_DIR in sys.path:
  sys.path.append(BASE_DIR)

from pymemcache.client.hash import HashClient
from pymemcache.client.rendezvous_load import RendezvousLoadHash

def main():
  if len(sys.argv) < 3:
    sys.stderr.write('usage: python latency.py CONCURRENCY COUNT ALPHA CLIENT_TYPE HOST...\n')
    sys.exit(1)

  concurrency = int(sys.argv[1])
  count = int(sys.argv[2])
  alpha = int(sys.argv[3])
  client_t = sys.argv[4]
  hosts = sys.argv[5:]

  parsed_hosts = []
  for h in hosts:
    host, port = h.split(':')
    parsed_hosts.append((host, int(port)))

  global_client = get_client(client_t, parsed_hosts)
  global_client.flush_all()

  for _ in range(count):
    key = str(numpy.random.zipf(alpha))
    value = hashlib.sha256(bytes(key)).digest()
    global_client.set(key, value)

  del global_client

  # Spawn multiple processes for each benchmark.
  processes = {}
  for _ in range(concurrency):
    parent_conn, child_conn = multiprocessing.Pipe(duplex=False)
    p = multiprocessing.Process(
      target=run_benchmark,
      args=(client_t, parsed_hosts, count, alpha, child_conn))

    processes[p] = parent_conn
    p.start()

  for p, conn in processes.items():
    p.join()

    for i in range(count):
      print(conn.recv())

  sys.exit(0)

def run_benchmark(client_t, hosts, count, alpha, conn):
  '''
  Run the benchmark.
  '''
  client = get_client(client_t, hosts)
  times = []

  for _ in range(count):
    key = str(numpy.random.zipf(alpha))

    start_time = time.time()
    client.get(key)
    elapsed = time.time() - start_time

    times.append(elapsed)

  for t in times:
    conn.send(t)

def get_client(client_t, hosts):
  '''
  Get a client with the given type.
  '''
  if client_t == 'hash':
    return HashClient(servers=hosts)
  elif client_t == 'load':
    return HashClient(servers=hosts, hasher=RendezvousLoadHash)
  else:
    sys.stderr.write('client_t must be one of: hash, load\n')
    sys.exit(1)

if __name__ == '__main__':
  main()
