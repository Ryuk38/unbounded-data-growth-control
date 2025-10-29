import gzip
import shutil
import os
import time

def compress_file(file_path, level=9, delete_original=True):
    """
    Compresses a file using gzip with a specified compression level.
    """
    if file_path.endswith(".gz"):
        return file_path, os.path.getsize(file_path)
        
    compressed_path = file_path + ".gz"

    with open(file_path, 'rb') as f_in:
        with gzip.open(compressed_path, 'wb', compresslevel=level) as f_out:
            shutil.copyfileobj(f_in, f_out)

    original_size = os.path.getsize(file_path)
    compressed_size = os.path.getsize(compressed_path)

    if delete_original:
        os.remove(file_path)

    print(f"Compressed {file_path} → {compressed_path} [Level {level}]")
    return compressed_path, compressed_size

def measure_decompression_latency(gz_file_path):
    """
    Measures the latency of reading the first 4KB of DECOMPRESSED data.
    This correctly measures the decompression penalty.
    """
    start_time = time.time()
    try:
        with gzip.open(gz_file_path, 'rb') as f:
            f.read(4096)  # Read 4KB of decompressed data
    except Exception as e:
        print(f"Error measuring decompression latency: {e}")
        return 0.0
        
    end_time = time.time()
    return (end_time - start_time) * 1000 # Return in ms