# latency.py
# Benchmark the memcached server latency across a Zipfian distribution with
# concurrent clients.

import os.path
import time
import sys
import hashlib
import multiprocessing
import tempfile
import shutil
import socket
import errno

from numpy.random import zipf

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
if not BASE_DIR in sys.path:
  sys.path.append(BASE_DIR)

from pymemcache.client.hash import HashClient
from pymemcache.client.rendezvous_load import RendezvousLoadHash
from pymemcache.load import rusage_load

def main():
  if len(sys.argv) < 3:
    sys.stderr.write('usage: python latency.py CONCURRENCY COUNT ALPHA CLIENT_TYPE HOST...\n')
    sys.exit(1)

  concurrency = int(sys.argv[1])
  count = int(sys.argv[2])
  alpha = float(sys.argv[3])
  # alpha of 1.135 corresponds to a 80-20 distribution
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
    os.remove(path)

  sys.exit(0)

def initialize_benchmark(client_t, parsed_hosts, count, alpha):
  '''
  Initialize the benchmark.
  '''
  client = get_client(client_t, parsed_hosts)
  client.flush_all()

  for _ in range(count):
    key = str(zipf(alpha))
    value = hashlib.sha256(bytes(key)).digest()
    client.set(key, value)

def run_benchmark(client_t, hosts, count, alpha, path):
  '''
  Run the benchmark.
  '''
  client = get_client(client_t, hosts)
  stream = open(path, 'w')

  for _ in range(count):
    key = str(zipf(alpha))

    status = 0
    start_time = time.time()
    try:
      client.get(key)
    except socket.error as e:
      status = e.errno
    elapsed = time.time() - start_time

    if status in {errno.ETIMEDOUT, errno.ECONNRESET}:
      # Client probably got botched, so reset the client for future requests.
      client = get_client(client_t, hosts)

    stream.write('%s,%f,%d' % (key, elapsed, status))
    stream.write('\n')

  stream.close()

def get_client(client_t, hosts):
  '''
  Get a client with the given type.
  '''
  if client_t == 'hash':
    return HashClient(servers=hosts)
  elif client_t == 'load':
    return HashClient(
      servers=hosts, hasher=RendezvousLoadHash)
  else:
    sys.stderr.write('client_t must be one of: hash, load\n')
    sys.exit(1)

if __name__ == '__main__':
  main()
