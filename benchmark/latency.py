# latency.py
# Benchmark the memcached server latency across a Zipfian distribution with
# concurrent clients.

import os.path
import time
import sys
import numpy.random
import hashlib
import multiprocessing
import tempfile
import shutil

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

  initialize_benchmark(client_t, parsed_hosts, count, alpha)
  
  # Spawn `concurrency` processes, one per concurrent client.
  start_time = time.time()
  processes = {} # maps processes to paths

  for _ in range(concurrency):
    # Temporary files are used as an easy way to get output from the clients.
    _, path = tempfile.mkstemp(text=True)

    p = multiprocessing.Process(
      target=run_benchmark,
      args=(client_t, parsed_hosts, count, alpha, path))

    processes[p] = path
    p.start()

  sys.stderr.write('Started %d processes.\n' % concurrency)

  for p in processes.keys():
    p.join()

  sys.stderr.write('All processes finished in %f seconds.\n' % (time.time() - start_time))

  for path in processes.values():
    with open(path, 'r') as stream:
      shutil.copyfileobj(stream, sys.stdout)

  sys.exit(0)

def initialize_benchmark(client_t, parsed_hosts, count, alpha):
  '''
  Initialize the benchmark.
  '''
  client = get_client(client_t, parsed_hosts)
  client.flush_all()

  for _ in range(count):
    key = str(numpy.random.zipf(alpha))
    value = hashlib.sha256(bytes(key)).digest()
    client.set(key, value)

def run_benchmark(client_t, hosts, count, alpha, path):
  '''
  Run the benchmark.
  '''
  client = get_client(client_t, hosts)
  stream = open(path, 'w')

  for _ in range(count):
    key = str(numpy.random.zipf(alpha))

    start_time = time.time()
    client.get(key)
    elapsed = time.time() - start_time

    stream.write(str(elapsed))
    stream.write('\n')

  stream.close()

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
