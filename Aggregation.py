import pandas as pd
import os

def aggregate_timeseries_data(csv_path, output_path, resample_freq='1H'):
    """
    Reads a large time-series CSV in chunks, aggregates it to a lower
    frequency, and saves the result.
    """
    chunk_iterator = pd.read_csv(
        csv_path,
        parse_dates=['timestamp'],
        index_col='timestamp',
        chunksize=100000  # Process 100,000 rows at a time
    )
    
    aggregated_chunks = []
    
    print(f"Starting aggregation of {csv_path} to {resample_freq} frequency...")
    
    for chunk in chunk_iterator:
        agg_rules = {
            'temperature': 'mean',
            'pressure': 'mean',
            'event_count': 'sum'
        }
        resampled_chunk = chunk.resample(resample_freq).agg(agg_rules)
        aggregated_chunks.append(resampled_chunk)
        
    # Concatenate & properly aggregate across chunks
    full_aggregated_df = pd.concat(aggregated_chunks)
    final_df = full_aggregated_df.groupby(full_aggregated_df.index).agg({
        'temperature': 'mean',
        'pressure': 'mean',
        'event_count': 'sum'
    })
    
    original_size = os.path.getsize(csv_path)
    final_df.to_csv(output_path)
    final_size = os.path.getsize(output_path)
    
    print("Aggregation complete.")
    print(f"Original size: {original_size} bytes")
    print(f"Aggregated size: {final_size} bytes")
    print(f"Reduction ratio: {original_size / final_size:.2f}x")
    
    return output_path
