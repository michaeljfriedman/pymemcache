# initialize.py
# Initialize the benchmark.py

import sys
import time
import os.path
import hashlib

from numpy.random import zipf

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
if not BASE_DIR in sys.path:
  sys.path.append(BASE_DIR)

from pymemcache.client.hash import HashClient

def main():
  if len(sys.argv) <= 4:
    sys.stderr.write('usage: python initialize.py COUNT ALPHA HOST...\n')
    sys.exit(1)

  count = int(sys.argv[1])
  alpha = float(sys.argv[2])
  # alpha of 1.135 corresponds to a 80-20 distribution
  hosts = sys.argv[3:]

  parsed_hosts = []
  for h in hosts:
    host, port = h.split(':')
    parsed_hosts.append((host, int(port)))

  client = HashClient(servers=parsed_hosts)
  start_time = time.time()
  keys = initialize_benchmark(client, count, alpha)
  elapsed = time.time() - start_time

  sys.stderr.write('Initialized benchmark in %fs\n' % elapsed)
  for k in keys:
    print(k)

def initialize_benchmark(client, count, alpha):
  '''
  Initialize the benchmark.
  '''
  client.flush_all()

  keys = set()
  for _ in range(count):
    k = zipf(alpha)
    key = str(k)
    client.set(key, hashlib.sha256(bytes(key)).digest())
    keys.add(k)

  return keys

if __name__ == '__main__':
  main()
