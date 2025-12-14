# Kaggle CSV Dataset Collector

A robust Python pipeline for **collecting, filtering, deduplicating, and indexing CSV datasets from Kaggle at scale**.

This project is designed for building a **high-quality tabular data pool** with strict constraints on dataset size, CSV structure, and per-dataset diversity.

---

## Features

- 🔍 Search Kaggle datasets by **multiple keywords & pages**
- 📦 Download datasets with **pre-check size limit (≤ 2GB per dataset)**
- 📊 Filter CSV files by:
  - Row count
  - Column count
  - Content hash (global deduplication)
- 🧠 **Per-dataset CSV selection (max 5)**
  - Prefer different table names (file-name based)
- 🧾 Generate a comprehensive `index.csv`
- 🧹 Automatic cleanup of temporary files
- 🔁 Built-in retry & rate-limit mitigation
- 🛡️ Handles **CSV filename encoding / garbled text issues**

---

## Output Structure

```
kaggle_pool/
├── all_csv/            # Final accepted CSV files
│   ├── sales_2022_a91c2f3e12.csv
│   ├── train_b83d91a44e.csv
│   └── ...
├── index.csv           # Metadata index of all collected CSVs
└── raw_datasets/       # Temporary downloads (auto-deleted)
```

---

## Filtering Rules

### Dataset-level
- **Total dataset size ≤ 2048 MB**
- If dataset size cannot be determined:
  - Can be allowed (configurable)
  - Still checked again after download

### CSV-level
| Constraint | Default |
|---|---|
| Min rows | 300 |
| Max rows | 50,000 |
| Min columns | 4 |
| Max CSVs per dataset | 5 |
| Deduplication | Global MD5 hash |

---

## Table Name Logic

In this project, **table name is derived from the CSV filename**, not from headers.

Example:

| Filename | Table name signature |
|---|---|
| `train_1.csv` | `train` |
| `train_2.csv` | `train` |
| `test.csv` | `test` |

Selection strategy:
1. Prefer CSVs with **different table name signatures**
2. If fewer than 5 are found, allow duplicates to fill up

---

## index.csv Schema

| Column | Description |
|---|---|
| `filename` | Final saved CSV filename |
| `rows` | Number of rows |
| `cols` | Number of columns |
| `size_kb` | File size (KB) |
| `md5` | Content hash |
| `source` | Kaggle dataset reference |
| `keyword` | Search keyword |
| `name_sig` | Normalized table name |
| `orig_zip_name` | Original filename inside zip |
| `fixed_zip_name` | Filename after encoding fix |

---

## Requirements

- Python 3.8+
- Kaggle CLI

Install Kaggle CLI:

```
pip install kaggle
```

---

## Authentication

A valid **Kaggle API Token** is required.

⚠️ **Never commit real API tokens to GitHub.**

---

## Usage

```
python kaggle_downlaod.py
```

The script is designed for **long-running execution** and tolerates:
- Network instability
- Kaggle API rate limiting
- Partial failures

---

## Disclaimer

This project uses the official Kaggle CLI.  
Users are responsible for complying with Kaggle’s Terms of Service.
