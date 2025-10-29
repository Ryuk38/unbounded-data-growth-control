import os
import time
from retention_policy_file import PolicyEngine
import compression
import Deduplication
import Aggregation
import log_rotation # This isn't strictly needed for the table, but good to have
import pandas as pd

# Track file size changes
report = []

def get_size(path):
    return os.path.getsize(path) if os.path.exists(path) else 0

# Store per-data-type stats
summary_stats = {
    "Raw Data": {"technique": "None", "orig": 0, "final": 0, "cpu_times": [], "latencies": []},
    "Redundant Data": {"technique": "Deduplication", "orig": 0, "final": 0, "cpu_times": [], "latencies": []},
    "Log Data": {"technique": "Gzip (level 9)", "orig": 0, "final": 0, "cpu_times": [], "latencies": []},
    "Time-Series Data": {"technique": "Hourly Aggregation", "orig": 0, "final": 0, "cpu_times": [], "latencies": []},
}

def classify_file(file):
    if file.endswith(".log") or file.endswith(".gz"):
        return "Log Data"
    elif file.endswith(".bin") or file.endswith(".meta"):
        return "Redundant Data"
    elif file.endswith(".csv") or file.endswith("_agg.csv"):
        return "Time-Series Data"
    elif file.endswith(".dat"):
        return "Raw Data"
    else:
        return "Raw Data" # Default

def measure_raw_latency(file_path):
    """Simulate read latency by timing a single 4KB read operation"""
    if not file_path or not os.path.exists(file_path):
        return 0.0
    try:
        start = time.time()
        with open(file_path, "rb") as f:
            f.read(4096)  # read first 4KB
        end = time.time()
        return (end - start) * 1000  # ms
    except Exception as e:
        print(f"Error measuring raw latency: {e}")
        return 0.0

def run_pipeline():
    global report
    engine = PolicyEngine("policy.json")
    print("\n=== Starting Hybrid Data Reduction Pipeline ===\n")

    # Clear dedupe store for a clean run
    if os.path.exists(Deduplication.chunk_store_dir):
        import shutil
        shutil.rmtree(Deduplication.chunk_store_dir)
        os.makedirs(Deduplication.chunk_store_dir, exist_ok=True)
        Deduplication.hash_index = {}
        print("Cleared deduplication chunk store for clean test run.")

    for file in os.listdir("test_data"):
        path = os.path.join("test_data", file)
        if not os.path.isfile(path):
            continue

        original_size = get_size(path)
        data_type = classify_file(file)
        
        # Skip files that are products of other files for this test
        if file.endswith(".gz") or file.endswith(".meta") or file.endswith("_agg.csv"):
            continue

        summary_stats[data_type]["orig"] += original_size

        status = engine.get_file_status(path)
        if not status:
            continue

        action = status["action"]
        print(f"[POLICY] {path} (Age: {status['age_days']:.1f}d) → Tier: {status['tier']}, Action: {action}")

        start_cpu = time.time()
        final_size = original_size
        final_path = path
        latency = 0.0

        try:
            if action == "compress":
                final_path, final_size = compression.compress_file(path, level=9)
                latency = compression.measure_decompression_latency(final_path)

            elif action == "deduplicate":
                # Returns the path to the .meta file and the true final size
                final_path, final_size = Deduplication.deduplicate_file(path)
                latency = Deduplication.measure_read_latency(final_path)
            
            elif action == "aggregate":
                if path.endswith(".csv"):
                    final_path = path.replace(".csv", "_agg.csv")
                    # aggregate_timeseries_data returns the final path and final_size
                    final_path, final_size = Aggregation.aggregate_timeseries_data(path, final_path)
                    latency = measure_raw_latency(final_path) # Aggregated file is raw, so this is fine

            elif action == "delete":
                os.remove(path)
                print(f"Deleted {path}")
                final_size = 0
            
            elif action == "none":
                # This is our baseline
                latency = measure_raw_latency(path)

        except Exception as e:
            print(f"[ERROR] Failed to process {path}: {e}")

        end_cpu = time.time()
        elapsed_cpu = end_cpu - start_cpu

        report.append({
            "file": file,
            "original_size": original_size,
            "final_size": final_size,
            "action": action
        })

        summary_stats[data_type]["final"] += final_size
        
        size_gb = (original_size / 1e9) if original_size > 0 else 1
        cpu_per_gb = elapsed_cpu / size_gb
        summary_stats[data_type]["cpu_times"].append(cpu_per_gb)

        if latency > 0:
            summary_stats[data_type]["latencies"].append(latency)

    print("\n=== Pipeline Complete ===")
    generate_summary_table()

def generate_summary_table():
    print("\n=== CORRECTED Paper-Style Summary Table ===\n")
    print(f"{'Data Type':<20} {'Technique Applied':<20} {'Reduction Ratio':<15} {'CPU Time (s/GB)':<20} {'Avg. Read Latency (ms)':<23}")
    print("-" * 98)

    for dtype, stats in summary_stats.items():
        orig = stats["orig"]
        final = stats["final"]
        
        if orig > 0 and final > 0:
            ratio = orig / final
        elif orig > 0 and final == 0:
            ratio = float('inf') # Deletion
        else:
            ratio = 1.0 # Baseline or no data

        avg_cpu = sum(stats["cpu_times"]) / len(stats["cpu_times"]) if stats["cpu_times"] else 0
        avg_lat = sum(stats["latencies"]) / len(stats["latencies"]) if stats["latencies"] else 0

        print(f"{dtype:<20} {stats['technique']:<20} {ratio:<15.1f} {avg_cpu:<20.2f} {avg_lat:<23.2f}")

if __name__ == "__main__":
    # Need to update Aggregation.py to return final_size
    # Quick patch here to modify the Aggregation.py function
    
    original_agg = Aggregation.aggregate_timeseries_data
    
    def patched_agg(csv_path, output_path, resample_freq='1H'):
        """Patched version to return path and size"""
        print(f"Starting aggregation of {csv_path} to {resample_freq} frequency...")
        chunk_iterator = pd.read_csv(
            csv_path,
            parse_dates=['timestamp'],
            index_col='timestamp',
            chunksize=100000
        )
        aggregated_chunks = []
        for chunk in chunk_iterator:
            agg_rules = {'temperature': 'mean', 'pressure': 'mean', 'event_count': 'sum'}
            resampled_chunk = chunk.resample(resample_freq).agg(agg_rules)
            aggregated_chunks.append(resampled_chunk)
        
        full_aggregated_df = pd.concat(aggregated_chunks)
        final_df = full_aggregated_df.groupby(full_aggregated_df.index).agg({
            'temperature': 'mean',
            'pressure': 'mean',
            'event_count': 'sum'
        })
        
        original_size = os.path.getsize(csv_path)
        final_df.to_csv(output_path)
        final_size = os.path.getsize(output_path)
        
        print(f"Aggregation complete. Original: {original_size} | Final: {final_size}")
        return output_path, final_size

    # Overwrite the old function with our patched one
    Aggregation.aggregate_timeseries_data = patched_agg
    
    run_pipeline()