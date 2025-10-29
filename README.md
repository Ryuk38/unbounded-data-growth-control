
---

# Controlling Unbounded Data Growth: A Hybrid Approach to Data Reduction and Lifecycle Management

This repository contains the official **Python reference implementation** for the research paper:
**“Controlling Unbounded Data Growth: A Hybrid Approach to Data Reduction and Lifecycle Management.”**

This project proposes and demonstrates a **hybrid, policy-driven framework** that integrates multiple data reduction strategies—including **block-level deduplication**, **adaptive compression**, and **time-series aggregation**—based on **data age, access frequency, and business value**.
The framework is designed to manage the full **data lifecycle** across **Hot**, **Warm**, and **Cold** tiers.

📄 **[Read the Full Research Paper (PDF)](https://github.com/Ryuk38/unbounded-data-growth-control/blob/master/Documentation/Controlling%20Unbounded%20Data%20Growth.pdf)**

---

## 🚀 Framework Overview

This repository provides a **multi-stage data processing pipeline** that applies progressively more aggressive reduction techniques as data ages.

### Core Components

* **Policy Engine:**
  A JSON-based engine (`policy.json`) defines rules for data classification, retention, and reduction actions.
* **Data Tiers:**
  The `PolicyEngine` (in `retention_policy_file.py`) classifies files into **Hot**, **Warm**, and **Cold** tiers based on file modification time (age).
* **Reduction Techniques:**
  The main pipeline (`main.py`) orchestrates all modules and performs automated actions:

  * **Compression** – Uses Gzip for log files (`compression.py`).
  * **Deduplication** – Performs block-level deduplication (`Deduplication.py`).
  * **Aggregation** – Reduces time-series CSV data granularity (`Aggregation.py`).

---

## 🛠️ Setup and Installation

### 1. Clone the Repository

```bash
git clone https://github.com/Ryuk38/unbounded-data-growth-control.git
cd unbounded-data-growth-control
```

### 2. Prerequisites

* Python 3.7+
* Windows environment (for running the PowerShell script `set_file_ages.ps1`)

### 3. Install Dependencies

Only one external dependency (`pandas`) is required:

```bash
pip install pandas
```

---

## 📊 Running the Experiment

You can replicate the quantitative results (from **Table II** in the paper) by following these steps:

> ⚠️ **Note:** This process generates approximately **11 GB** of synthetic test data. Ensure you have sufficient disk space.

### Step 1: Generate the Test Dataset

This creates the `test_data` directory with logs, binary files, and a large CSV file.

```bash
python generate_test_data_10gb.py
```

### Step 2: Simulate File Aging (Windows PowerShell)

The PowerShell script updates file modification times to simulate real-world data aging.

```powershell
.\set_file_ages.ps1
```

### Step 3: Run the Main Reduction Pipeline

The pipeline reads `policy.json`, scans the data directory, and applies the appropriate reduction techniques.

```bash
python main.py
```

---

## 📈 Expected Output

After execution, the program prints a **performance summary table** (replicating Table II from the paper):

```
=== Paper-Style Summary Table ===

Data Type            Technique Applied    Reduction Ratio   CPU Time (s/GB)   Avg. Read Latency (ms)
--------------------------------------------------------------------------------------------------
Raw Data             None                 1.0               0.03              37.52
Redundant Data       Deduplication        59.4              2.86              161.07
Log Data             Gzip (level 9)       257.8             8.81              38.92
Time-Series Data     Hourly Aggregation   2123.0            41.44             25.43
```

> *Note: Actual results may vary depending on your hardware.*

---

## 📁 Repository Structure

```
.
├── .git/                       # Git version control directory
├── __pycache__/                # Compiled Python cache files
├── chunk_store/                # Storage for deduplicated data chunks
├── Documentation/              # Folder containing documentation or paper resources
├── test_data/                  # Synthetic dataset generated for experiments
│
├── .gitignore                  # Git ignore rules
├── Aggregation.py              # Module for time-series aggregation
├── compression.py              # Module for Gzip compression
├── Deduplication.py            # Module for block-level deduplication
├── generate_test_data_10gb.py  # Script to generate the synthetic dataset (~10GB)
├── log_rotation.py             # Module simulating log rotation (similar to logrotate)
├── main.py                     # Main orchestrator for the data reduction pipeline
├── policy.json                 # Declarative policy configuration file
├── retention_policy_file.py    # Implementation of the PolicyEngine class
├── set_file_ages.ps1           # PowerShell script to simulate file aging

```

---

## 📄 Citation

If you use this repository or its concepts in your research, please cite:

```
Bineesh Mathew (2025). Controlling Unbounded Data Growth: 
A Hybrid Approach to Data Reduction and Lifecycle Management. 
St. Xavier's College, Mumbai.
```

---

## ⚖️ License

This project is released under the **MIT License**.
You are free to use, modify, and distribute this work with appropriate attribution.

---

**Author:** *Bineesh Mathew*
📧 [c380bineesh@gmail.com](mailto:c380bineesh@gmail.com)
🏫 St. Xavier’s College, Mumbai
🔗 [GitHub Repository](https://github.com/Ryuk38/unbounded-data-growth-control)

---


