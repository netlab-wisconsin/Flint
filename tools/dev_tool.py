#!/usr/bin/python
import os
import argparse
import sys
import subprocess

ROCKSDB_HOME = '/tmp/flint_db'
VOLUME_CF_PREFIX = '/volume_meta/'
BDEV_TOOL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../build/tools/ssd_tool')

ARBITER_METADATA_CF = '/flint_arbiter'

def parse_args():
  parser = argparse.ArgumentParser(__file__)
  parser.add_argument('--reset', action='store_true', default=False,
                      help='Reset rocksdb')
  parser.add_argument('--show-meta', action='store_true', default=False,
                      help='Show contents of volume_meta column family')
  parser.add_argument('--show-volume', type=str, default="",
                      help='Show volume attributes')
  if len(sys.argv) == 1:
    parser.print_help()
    exit(1)
  return parser.parse_args()


def list_column_families(db_path):
    """Returns a list of column families in the RocksDB database."""
    result = subprocess.run(["ldb", "--db=" + db_path, "list_column_families"], 
                            capture_output=True, text=True)
    if result.returncode != 0:
        print("Error listing column families:", result.stderr)
        return []
    return result.stdout.strip().split("\n")
  

if __name__ == '__main__':
  args = parse_args()
  if args.reset:
    os.system('rm %s/*' % ROCKSDB_HOME)
    os.system('ldb --db=%s repair' % ROCKSDB_HOME)
    os.system('sudo %s --format-all' % BDEV_TOOL_PATH)
  if args.show_meta:
    os.system('ldb --db=%s --column_family=%s scan' % (ROCKSDB_HOME, ARBITER_METADATA_CF))
  if args.show_volume != "":
    os.system('ldb --db=%s --column_family=%s scan' % (ROCKSDB_HOME, VOLUME_CF_PREFIX + args.show_volume))