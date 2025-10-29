import hashlib
import os
import json
import time

CHUNK_SIZE = 4096
hash_index = {}
chunk_store_dir = 'chunk_store'
os.makedirs(chunk_store_dir, exist_ok=True)

def deduplicate_file(file_path):
    """
    Performs block-level deduplication on a file.
    Returns a metadata file path and the space saved.
    """
    original_size = os.path.getsize(file_path)
    metadata = {'chunks': []}
    space_saved = 0
    new_chunks_size = 0

    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            
            chunk_hash = hashlib.sha256(chunk).hexdigest()
            metadata['chunks'].append(chunk_hash)

            if chunk_hash not in hash_index:
                chunk_file_path = os.path.join(chunk_store_dir, chunk_hash)
                with open(chunk_file_path, 'wb') as chunk_file:
                    chunk_file.write(chunk)
                hash_index[chunk_hash] = chunk_file_path
                new_chunks_size += len(chunk)
            else:
                space_saved += len(chunk)

    metadata_path = file_path + '.meta'
    with open(metadata_path, 'w') as meta_file:
        json.dump(metadata, meta_file)

    # Calculate final size on disk (metadata file + new chunks)
    # This is a more accurate measure for the summary table
    final_size = os.path.getsize(metadata_path) + new_chunks_size
    
    os.remove(file_path)
    print(f"Deduplicated {file_path}. Original: {original_size}, Final: {final_size}, Saved: {space_saved}")
    
    # Return the final size, not just space saved, for easier reporting
    return metadata_path, final_size

def measure_read_latency(metadata_path, chunk_store='chunk_store'):
    """
    Measures the latency of reading the first chunk of a deduplicated file.
    This simulates the read penalty described in the paper.
    """
    start_time = time.time()
    try:
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        first_chunk_hash = metadata['chunks'][0]
        chunk_path = os.path.join(chunk_store, first_chunk_hash)
        
        with open(chunk_path, 'rb') as f_chunk:
            f_chunk.read(CHUNK_SIZE)
            
    except Exception as e:
        print(f"Error measuring dedupe latency: {e}")
        return 0.0
        
    end_time = time.time()
    return (end_time - start_time) * 1000 # Return in ms