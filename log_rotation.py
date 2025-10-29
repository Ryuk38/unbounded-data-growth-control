import os
import glob
import gzip
import shutil
import time
from datetime import datetime

def compress_file(file_path, level=9):
    """Compress a file with gzip and remove the original."""
    with open(file_path, 'rb') as f_in:
        with gzip.open(file_path + '.gz', 'wb', compresslevel=level) as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(file_path)
    print(f"Compressed: {file_path} → {file_path}.gz")

def manage_logs(log_dir, rotation_days=1, compression_days=7, retention_days=30):
    """
    Rotates, compresses, and prunes log files in a directory.
    """
    # Rotate active log file if it's older than rotation_days
    active_log = os.path.join(log_dir, 'app.log')
    if os.path.exists(active_log):
        mod_time = os.path.getmtime(active_log)
        if (time.time() - mod_time) / (24 * 3600) > rotation_days:
            timestamp = datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d_%H%M%S')
            rotated_log_path = os.path.join(log_dir, f'app.log.{timestamp}')
            os.rename(active_log, rotated_log_path)
            print(f"Rotated {active_log} → {rotated_log_path}")

    # Compress old, uncompressed logs (but not already compressed ones)
    for log_file in glob.glob(os.path.join(log_dir, 'app.log.*')):
        if not log_file.endswith('.gz'):
            mod_time = os.path.getmtime(log_file)
            if (time.time() - mod_time) / (24 * 3600) > compression_days:
                compress_file(log_file, level=9)

    # Prune logs older than retention_days
    for log_file in glob.glob(os.path.join(log_dir, 'app.log.*.gz')):
        mod_time = os.path.getmtime(log_file)
        if (time.time() - mod_time) / (24 * 3600) > retention_days:
            os.remove(log_file)
            print(f"Pruned old log file: {log_file}")
