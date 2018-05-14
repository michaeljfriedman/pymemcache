# pymemcache/load.py
# Author: Rushy Panchal
# Date: May 6th, 2018
# Description: Manages load statistics for a set of memcached servers.

import pymemcache.client.base

import collections
import threading
import time
import math
import socket

def rusage_load(stats, previous_load, elapsed_time):
  '''
  Get load information based on the rusage (CPU time used) both for the system
  and the user.
  '''
  current_load = (float(stats.get('rusage_user', 0))
                + float(stats.get('rusage_system', 0)))
  return (current_load, float(current_load - previous_load) / elapsed_time)

def cum_req_load(stats, previous_num_reqs, elapsed_time):
  '''
  Get load information based on the cumulative number of requests handled by
  this server. Returns current total number of requestss, and average requests
  per second over the elapsed_time interval.
  '''
  current_num_reqs = (float(stats.get('cmd_get', 0))
                   + float(stats.get('cmd_set', 0))
                   + float(stats.get('cmd_flush', 0))
                   + float(stats.get('cmd_touch', 0)))
  return (current_num_reqs,
    float(current_num_reqs - previous_num_reqs) / elapsed_time)

class LoadManager(object):
  '''
  Manage load statistics for a set of memcached servers. Provided servers have
  their load refreshed periodically.

  `refresh_rate` is how frequently the data is refreshed (in seconds).

  `load_metric` is the key function to map server statistics to a load indicator
  (as a float).

  `window_size` is the size of the window used for calculating the moving
  average of the load.
  '''
  def __init__(self,
      refresh_rate=1,
      load_metric=cum_req_load,
      window_size=100):
    self._refresh_rate = int(refresh_rate)
    self._load_metric = load_metric
    self._window_size = window_size

    # This lock is held solely on _servers.
    self._server_lock = threading.Lock()
    self._servers = collections.defaultdict(
      lambda: {
        'client': None,
        'load': 0,
        'moving_statistics': MovingStatistics(self._window_size),
        'last_updated': 0,
        })

    # The lock is held on _moving_statistics and _inst_load.
    # NOTE: This lock should *not* be held in conjunction with _server_lock.
    #       Locks are held with mutual exclusion to prevent any deadlock
    #       situation from arising. Locks should always be acquired in the
    #       order of _server_lock, _data_lock.
    self._data_lock = threading.Lock()

    # This data provides a 'view' into the data stored per-server, which
    # allows for faster returns to the client requesting load information.
    # In particular, it also allows the locks to be more granular.
    self._moving_statistics = collections.defaultdict(lambda: {
      'average': 0,
      'stddev': 0,
      })
    self._inst_load = collections.defaultdict(int)

    self._thread = threading.Thread(target=self._periodic_load_update)

    # Start the thread as a daemon so it can run in the background without
    # preventing the main process from exiting.
    self._thread.daemon = True
    self._thread.start()

  def add_server(self, key, server):
    '''
    Add a server to the manager.
    '''
    with self._server_lock:
      s = self._servers[key]
      s['client'] = pymemcache.client.base.Client(server)

  def remove_server(self, key):
    '''
    Remove a server from the manager.
    '''
    with self._server_lock:
      cl =  self._servers[key]['client']
      if cl:
        cl.close()

      del self._servers[key]

    with self._data_lock:
      del self._moving_statistics[key]
      del self._inst_load[key]

  def load(self):
    '''
    Get the current load information.
    '''
    with self._data_lock:
      return self._inst_load
 
  def load_statistics(self):
    '''
    Get the average load over the window size.
    '''
    with self._data_lock:
      return self._moving_statistics

  def _periodic_load_update(self):
    '''
    Periodically update the load for all of the servers.
    '''
    while True:
      self._update_load()
      time.sleep(self._refresh_rate)

  def _update_load(self):
    '''
    Update the load information for all of the servers.
    '''
    # The updates are performed in a thread-specific data structure so that the
    # two locks are not held simultaneously. In addition, it prevents one
    # server from having more up-to-date information than another (unless there
    # is an error).
    updated_inst_loads = {}
    updated_moving_statistics = {}

    with self._server_lock:
      for key, server in self._servers.items():
        try:
          server_stats = server['client'].stats()
        except (socket.error, TypeError):
          continue
        else:
          try:
            elapsed_time = server_stats['uptime'] - server['last_updated']
          except KeyError:
            elapsed_time = self._refresh_rate
          total_load, current_load = self._load_metric(
            server_stats, server['load'], elapsed_time)

          server['load'] = total_load
          server['last_updated'] += elapsed_time

          moving_stats = server['moving_statistics']
          moving_stats.add_point(current_load)

          updated_inst_loads[key] = current_load
          updated_moving_statistics[key] = {
            'average': moving_stats.average(),
            'stddev': moving_stats.stddev(),
            }

    with self._data_lock:
      self._inst_load.update(updated_inst_loads)
      self._moving_statistics.update(updated_moving_statistics)

class MovingStatistics(object):
  '''
  Calculate moving averages over data of a window size `window_size`.
  '''
  def __init__(self, window_size):
    self._window_size = window_size
    self._average = 0
    self._stddev = 0
    self._window = collections.deque(maxlen=window_size)

  def average(self):
    '''
    Get the current moving average.
    '''
    return self._average

  def stddev(self):
    '''
    Get the current standard deviation.
    '''
    return self._stddev

  def add_point(self, x):
    '''
    Add a point to the moving average.
    '''
    if len(self._window) + 1 == self._window_size:
      removed = self._window.popleft()
      self._average -= float(removed) / self._window_size

      self._stddev = math.sqrt(sum(map(
        lambda x: float(self._average - x)**2,
        self._window)) / (self._window_size - 1))

    self._window.append(x)
    self._average += float(x) / self._window_size
