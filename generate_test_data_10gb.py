import os
import random
import csv
import datetime

os.makedirs("test_data", exist_ok=True)

# ===============================
# 1) Generate ~1GB Raw Baseline File
# ===============================
def generate_raw_baseline_file():
    print("[*] Generating ~1GB raw baseline file...")
    target_size = 1 * 1024**3  # 1 GB
    chunk_size = 1024 * 1024 # 1 MB
    chunks = target_size // chunk_size
    
    with open("test_data/raw_baseline.dat", "wb") as f:
        for _ in range(chunks):
            f.write(os.urandom(chunk_size))
    print(f"[OK] Raw baseline file generated (~1GB)")

# ===============================
# 2) Generate ~4GB Log Files
# ===============================
def generate_large_logs():
    print("[*] Generating ~4GB log files...")
    log_templates = [
        "2025-09-02 12:00:00,123 - INFO - User logged in\n",
        "2025-09-02 12:00:01,456 - WARN - Disk space low\n",
        "2025-09-02 12:00:02,789 - ERROR - Connection timeout\n",
        "2025-09-02 12:00:03,321 - DEBUG - Cache refreshed\n"
    ]
    line_size = len(log_templates[0].encode("utf-8"))
    target_size = 4 * 1024**3  # 4 GB total
    lines_needed = target_size // line_size

    with open("test_data/app.log", "w") as f:
        for i in range(lines_needed // 2):
            f.write(log_templates[i % len(log_templates)])

    with open("test_data/app2.log", "w") as f:
        for i in range(lines_needed // 2):
            f.write(log_templates[i % len(log_templates)])
    print(f"[OK] Logs generated: app.log + app2.log (~4GB, compressible)")

# ===============================
# 3) Generate ~4GB Redundant BINs
# ===============================
def generate_redundant_bins():
    print("[*] Generating ~4GB redundant binary files...")
    # Generate 1 MB random block
    chunk = os.urandom(1024 * 1024)  # 1 MB
    target_size = (4 * 1024**3) // 3
    repeats = target_size // len(chunk)

    for i in range(1, 4):
        with open(f"test_data/redundant{i}.bin", "wb") as f:
            for _ in range(repeats):
                f.write(chunk)
    print(f"[OK] Redundant BIN files generated (~4GB, highly duplicate)")

# ===============================
# 4) Generate ~2GB Time-Series CSV
# ===============================
def generate_time_series_csv():
    print("[*] Generating ~2GB time-series CSV...")
    start = datetime.datetime(2024, 1, 1, 0, 0, 0)
    rows = 20_000_000
    with open("test_data/sensor.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "temperature", "pressure", "event_count"])
        for i in range(rows):
            ts = start + datetime.timedelta(seconds=i)
            writer.writerow([
                ts.strftime("%Y-%m-%d %H:%M:%S"),
                round(20 + random.random() * 5, 2),
                round(1000 + random.random() * 50, 2),
                random.randint(0, 100)
            ])
    print(f"[OK] Time-series CSV generated (~2GB)")

# ===============================
# Main
# ===============================
if __name__ == "__main__":
    generate_raw_baseline_file() # New
    generate_large_logs()
    generate_redundant_bins()
    generate_time_series_csv()
    print("\n=== ~11GB Dataset Ready ===")