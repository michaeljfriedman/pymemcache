# pymemcache/load.py
# Author: Rushy Panchal
# Date: May 6th, 2018
# Description: Manages load statistics for a set of memcached servers.

import collections
import threading
import time

def _rusage_load(stats, previous_load, elapsed_time):
  '''
  Get load information based on the rusage (CPU time used) both for the system
  and the user.
  '''
  current_load = (float(stats.get('rusage_user', 0))
                + float(stats.get('rusage_system', 0)))
  return (current_load, float(current_load - previous_load) / elapsed_time)

class LoadManager(object):
  '''
  Manage load statistics for a set of memcached servers. Provided servers have
  their load refreshed periodically.

  `refresh_rate` is how frequently the data is refreshed (in seconds).

  `key` is the key function to map server statistics to a load indicator (as a
  float).

  `window_size` is the size of the window used for calculating the moving
  average of the load.
  '''
  def __init__(self, refresh_rate=1, key=_rusage_load, window_size=10):
    self._servers = {}
    self._refresh_rate = int(refresh_rate)
    self._load_key = key
    self._window_size = window_size

    self._data_lock = threading.Lock()
    self._moving_averages = {}
    self._inst_load = {}
    self._load = {}
    self._last_updated = {}

    self._thread = threading.Thread(target=self._periodic_load_update)

    # Start the thread as a daemon so it can run in the background without
    # preventing the main process from exiting.
    self._thread.daemon = True
    self._thread.start()

  def add_server(self, key, client):
    '''
    Add a server to the manager.
    '''
    with self._data_lock:
      self._servers[key] = client
      self._moving_averages[key] = MovingAverage(self._window_size)
      self._inst_load[key] = 0
      self._load[key] = 0
      self._last_updated[key] = 0

  def remove_server(self, key):
    '''
    Remove a server from the manager.
    '''
    with self._data_lock:
      del self._moving_averages[key]
      del self._inst_load[key]
      del self._load[key]
      del self._last_updated[key]
      del self._servers[key]

  def load(self):
    '''
    Get the current load information.
    '''
    with self._data_lock:
      return self._inst_load

  def average_load(self):
    '''
    Get the average load over the window size.
    '''
    with self._data_lock:
      return {key: ma.average() for key, ma in self._moving_averages.items()}

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
    for key, server in self._servers.items():
      with self._data_lock:
        previous_load = self._load[key]
        previous_time = self._last_updated[key]

      current_stats = server.stats()
      elapsed_time = current_stats['uptime'] - previous_time
      total_load, current_load = self._load_key(
        current_stats, previous_load, elapsed_time)

      with self._data_lock:
        self._inst_load[key] = current_load
        self._load[key] = total_load
        self._last_updated[key] = current_stats['uptime']
        self._moving_averages[key].add_point(current_load)

class MovingAverage(object):
  '''
  Calculate moving averages over data of a window size `window_size`.
  '''
  def __init__(self, window_size):
    self._window_size = window_size
    self._current_size = 0
    self._current_average = 0
    self._window = collections.deque(maxlen=window_size)

  def average(self):
    '''
    Get the current moving average.
    '''
    return self._current_average

  def add_point(self, x):
    '''
    Add a point to the moving average.
    '''
    if len(self._window) + 1 == self._window_size:
      removed = self._window.popleft()
      self._current_average -= float(removed) / self._window_size

    self._window.append(x)
    self._current_average += float(x) / self._window_size
