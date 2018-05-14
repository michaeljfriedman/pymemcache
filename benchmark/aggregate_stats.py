# aggregate_stats.py
# Author: Michael Friedman
#
# Aggregate data from each given CSV files about client request latency into
# average, median, 5th percentile, and 95th percentile. Output results into
# one file, on separate lines:
#   avg,median,5th_percentile,95th_percentile

import csv
import sys
import numpy as np

def main():
  data_file_prefix = sys.argv[1]
  data_files = sys.argv[2:]

  # Read data
  sys.stderr.write('Reading data...\n')
  keys_by_file = {d: list() for d in data_files}
  latencies_by_file = {d: list() for d in data_files}
  for d in latencies_by_file.keys():
    with open(d) as f:
      reader = csv.reader(f)
      for row in reader:
        keys_by_file[d].append(int(row[0]))
        latencies_by_file[d].append(float(row[1]))

  # Compute stats for each file: avg, med, 95th percentile, 99.9th percentile
  sys.stderr.write('Computing stats...\n')
  agg_stats = []
  for d in data_files:
    # Filter latencies for popular keys (i.e. keys in the top 20% of all
    # requests).
    popularity_thresh = np.percentile(keys_by_file[d], 80)
    filtered = [l for k, l in zip(keys_by_file[d], latencies_by_file[d]) if k <= popularity_thresh]
    num_clients = int(d.replace(data_file_prefix, ''))
    avg = np.average(filtered)
    med = np.percentile(filtered, 50)
    low = np.percentile(filtered, 95)
    high = np.percentile(filtered, 99.9)
    agg_stats.append([num_clients, avg, med, low, high])

  # Write results to csv
  writer = csv.writer(sys.stdout)
  for row in agg_stats:
    writer.writerow(row)

if __name__ == '__main__':
  main()
