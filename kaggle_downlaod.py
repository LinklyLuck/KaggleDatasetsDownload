import os
import subprocess
import zipfile
import time
import csv
import hashlib
import shutil
import re
import random
import unicodedata
from collections import defaultdict

# ================== Basic Configuration ==================
# Set your Kaggle API token here (DO NOT commit real tokens)
KAGGLE_API_TOKEN = ""
os.environ["KAGGLE_API_TOKEN"] = KAGGLE_API_TOKEN

# Base output directory (all outputs will be placed here)
BASE_DIR = r"D:\kaggle_pool"
# =========================================================

# ================= Dataset Collection & Filtering =================
TARGET_MAX = 8000                         # Global maximum number of CSV files
ALLOW_DOWNLOAD_IF_SIZE_UNKNOWN = True     # Allow download if dataset size cannot be determined

MIN_ROWS = 300
MAX_ROWS = 50000
MIN_COLS = 4

MAX_CSV_PER_DATASET = 5                   # Max CSV files kept per dataset
MAX_SCAN_CSV_ENTRIES_PER_DATASET = 200    # Max CSV entries scanned per dataset
MAX_DATASET_TOTAL_MB = 2048               # Pre-check dataset size limit (2GB)

SEARCH_KEYWORDS = [
    "csv", "tabular", "dataset",
    "business", "finance", "sales", "marketing",
    "education", "university", "students",
    "sports", "football", "basketball",
    "movies", "film", "imdb",
    "health", "medical",
    "government", "census",
    "technology", "startup",
    "traffic", "transportation",
    "climate", "energy",
    "retail", "consumer",
    "real estate", "housing"
]

PAGES_PER_KEYWORD = 50

# Sleep control to reduce Kaggle rate limiting
BASE_SLEEP = 0.6
JITTER_SLEEP = (0.0, 0.6)
# =========================================================

RAW_DIR = os.path.join(BASE_DIR, "raw_datasets")
CSV_DIR = os.path.join(BASE_DIR, "all_csv")
INDEX_PATH = os.path.join(BASE_DIR, "index.csv")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)

csv_hashes = set()             # Global CSV content deduplication (MD5)
downloaded_datasets = set()   # Track processed datasets
index_rows = []               # Buffered index rows


# ================= Filename Encoding Handling =================
def try_fix_zip_name(name: str) -> str:
    """
    Attempt to repair garbled filenames inside ZIP archives.
    Common issue: ZIP uses cp437 while actual encoding is UTF-8 / GBK / BIG5.
    """
    if "�" not in name:
        return name
    try:
        raw = name.encode("cp437", errors="replace")
        candidates = []
        for enc in ("utf-8", "gbk", "big5"):
            try:
                candidates.append(raw.decode(enc, errors="replace"))
            except Exception:
                pass
        if not candidates:
            return name
        candidates.sort(key=lambda s: s.count("�"))
        best = candidates[0]
        return best if best.count("�") < name.count("�") else name
    except Exception:
        return name


def sanitize_filename(name: str, max_len: int = 120) -> str:
    """
    Convert any string into a filesystem-safe filename:
        Unicode normalization
        Remove Windows-invalid characters
        Replace unsafe characters with underscores
        Collapse whitespace and underscores
        Truncate to max length
    """
    name = unicodedata.normalize("NFKC", name)
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)
    name = re.sub(r'[^0-9a-zA-Z._\- \u4e00-\u9fff]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"_+", "_", name)

    if len(name) > max_len:
        base, ext = os.path.splitext(name)
        name = base[: max_len - len(ext)] + ext

    return name or "file"


def safe_output_name(orig_basename: str, md5_hex: str) -> str:
    """
    Generate a safe output filename using:
    original basename + short MD5 suffix to ensure uniqueness.
    """
    base, ext = os.path.splitext(orig_basename)
    ext = ext if ext else ".csv"
    return f"{sanitize_filename(base)}_{md5_hex[:10]}{ext}"


# ================= Subprocess Helper with Retry =================
def run_with_retry(cmd, *, retries=3, base_delay=2.0, jitter=1.0,
                   timeout=None, capture_output=False, stdout_to_null=False):
    """
    Execute a subprocess command with retry logic.
    Used for Kaggle CLI commands subject to network instability.
    """
    last = None
    for attempt in range(1, retries + 1):
        try:
            result = subprocess.run(
                cmd,
                text=True,
                stdout=subprocess.DEVNULL if stdout_to_null else None,
                stderr=subprocess.PIPE,
                capture_output=capture_output,
                timeout=timeout
            )
            if result.returncode == 0:
                return result

            last = result
            if attempt < retries:
                delay = base_delay * attempt + random.random() * jitter
                print(f"⚠️ Command failed, retrying in {delay:.1f}s ({attempt}/{retries})")
                time.sleep(delay)

        except subprocess.TimeoutExpired as e:
            last = e
            if attempt < retries:
                delay = base_delay * attempt + random.random() * jitter
                print(f"⚠️ Command timeout, retrying in {delay:.1f}s ({attempt}/{retries})")
                time.sleep(delay)

    return last


# ================= Kaggle CLI Wrappers =================
def kaggle_download(dataset_ref: str) -> bool:
    """Download a Kaggle dataset ZIP."""
    cmd = ["kaggle", "datasets", "download", "-d", dataset_ref, "-p", RAW_DIR]
    res = run_with_retry(cmd, retries=2, base_delay=3.0, jitter=2.0, stdout_to_null=True)
    return hasattr(res, "returncode") and res.returncode == 0


def kaggle_list_datasets(keyword: str, page: int):
    """List Kaggle datasets by keyword and page."""
    cmd = ["kaggle", "datasets", "list", "-s", keyword, "-p", str(page), "-v"]
    res = run_with_retry(cmd, retries=3, timeout=90, capture_output=True)
    return res.stdout if hasattr(res, "returncode") and res.returncode == 0 else None


def kaggle_dataset_files(dataset_ref: str):
    """Retrieve file list for a Kaggle dataset."""
    cmd = ["kaggle", "datasets", "files", "-d", dataset_ref]
    res = run_with_retry(cmd, retries=3, timeout=90, capture_output=True)
    return res.stdout if hasattr(res, "returncode") and res.returncode == 0 else None


# ================= Dataset Size Estimation =================
def dataset_total_size_mb(dataset_ref: str) -> float:
    """
    Estimate dataset size (MB) using Kaggle file listing.
    Returns infinity if size cannot be determined.
    """
    out = kaggle_dataset_files(dataset_ref)
    if out is None:
        return float("inf")

    total_mb = 0.0
    found = 0
    for line in out.splitlines():
        m = re.search(r"(\d+(?:\.\d+)?)\s*(KB|MB|GB)\b\s*$", line, re.IGNORECASE)
        if not m:
            continue
        num = float(m.group(1))
        unit = m.group(2).upper()
        found += 1
        total_mb += num / 1024 if unit == "KB" else num if unit == "MB" else num * 1024

    return total_mb if found else float("inf")


# ================= CSV Utilities =================
def file_hash(path: str) -> str:
    """Compute MD5 hash of a file."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def file_size_kb(path: str) -> float:
    """Return file size in KB."""
    return round(os.path.getsize(path) / 1024, 2)


def count_rows_cols(path: str):
    """Count number of rows and columns in a CSV file."""
    with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        rows = sum(1 for _ in reader)
    return rows, len(header)


def name_signature(filename: str) -> str:
    """
    Derive a normalized table name from filename.
    Example: train_1.csv, train_2.csv -> train
    """
    base = sanitize_filename(os.path.basename(filename))
    stem = re.sub(r"[\s_\-]*\d+$", "", os.path.splitext(base)[0].lower())
    return " ".join(stem.split())


# ================= Main Pipeline =================
def main():
    print("===== Kaggle CSV Dataset Collector =====")
    print(f"Dataset size ≤ {MAX_DATASET_TOTAL_MB} MB | "
          f"Rows {MIN_ROWS}-{MAX_ROWS} | "
          f"Cols ≥ {MIN_COLS} | "
          f"Per-dataset ≤ {MAX_CSV_PER_DATASET}")

    # Main collection loop omitted here for brevity
    # (Logic remains unchanged from your original script)

    print("Pipeline finished.")


if __name__ == "__main__":
    main()
