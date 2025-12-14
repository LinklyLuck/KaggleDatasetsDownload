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
KAGGLE_API_TOKEN = ""
os.environ["KAGGLE_API_TOKEN"] = KAGGLE_API_TOKEN

BASE_DIR = r"D:\kaggle_pool"   # Put all outputs under this directory on drive D:
# =========================================================

# ================= Collection Targets & Filtering Params =================
TARGET_MAX = 8000
ALLOW_DOWNLOAD_IF_SIZE_UNKNOWN = True

MIN_ROWS = 300
MAX_ROWS = 50000
MIN_COLS = 4

MAX_CSV_PER_DATASET = 5                   # Save at most 5 CSVs per dataset
MAX_SCAN_CSV_ENTRIES_PER_DATASET = 200    # Scan at most N CSV entries per dataset (fast + sufficient)
MAX_DATASET_TOTAL_MB = 2048               # Pre-check: only download datasets <= 2GB

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

# Base sleep between datasets (plus random jitter to reduce rate-limiting risk)
BASE_SLEEP = 0.6
JITTER_SLEEP = (0.0, 0.6)  # Add random 0~0.6 seconds
# =========================================================

RAW_DIR = os.path.join(BASE_DIR, "raw_datasets")
CSV_DIR = os.path.join(BASE_DIR, "all_csv")
INDEX_PATH = os.path.join(BASE_DIR, "index.csv")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)

csv_hashes = set()            # Global dedup by content MD5
downloaded_datasets = set()
index_rows = []


# ------------------------------ Filename encoding repair (new) ------------------------------
def try_fix_zip_name(name: str) -> str:
    """
    Attempt to repair filenames inside ZIP archives (not guaranteed 100%).
    Common case: ZIP is decoded as CP437 but actual encoding is UTF-8 / GBK / BIG5.
    """
    # If there is no replacement character, keep as-is
    if "�" not in name:
        return name
    try:
        raw = name.encode("cp437", errors="replace")
        candidates = []
        for enc in ("utf-8", "gbk", "big5"):
            try:
                fixed = raw.decode(enc, errors="replace")
                candidates.append(fixed)
            except Exception:
                pass
        if not candidates:
            return name
        # Choose the one with the fewest replacement characters
        candidates.sort(key=lambda s: s.count("�"))
        best = candidates[0]
        # If not actually better, don't change it
        if best.count("�") >= name.count("�"):
            return name
        return best
    except Exception:
        return name


def sanitize_filename(name: str, max_len: int = 120) -> str:
    """
    Convert any string into a Windows-safe filename:
      - Unicode normalization
      - Remove Windows-invalid/control characters
      - Replace uncommon characters with '_'
      - Collapse extra spaces/underscores
      - Truncate to max_len
    """
    name = unicodedata.normalize("NFKC", name)

    # Remove Windows-invalid characters and control chars
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)

    # Keep only a safe character set; replace others with '_'
    name = re.sub(r'[^0-9a-zA-Z._\- \u4e00-\u9fff]+', "_", name)

    # Collapse whitespace/underscores
    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"_+", "_", name)

    # Truncate
    if len(name) > max_len:
        base, ext = os.path.splitext(name)
        name = base[: max_len - len(ext)] + ext

    return name or "file"


def safe_output_name(orig_basename: str, md5_hex: str) -> str:
    """
    Generate a safe filename using:
      original basename (may be garbled) + short md5 suffix
    to avoid collisions and encoding issues.
    """
    base, ext = os.path.splitext(orig_basename)
    ext = ext if ext else ".csv"
    safe_base = sanitize_filename(base)
    suffix = md5_hex[:10]
    return f"{safe_base}_{suffix}{ext}"


# ------------------------------ Generic helper: run command with retry ------------------------------
def run_with_retry(cmd, *, retries=3, base_delay=2.0, jitter=1.0, timeout=None,
                   capture_output=False, stdout_to_null=False):
    """
    For commands like kaggle list/files that may fail intermittently due to network issues.
    """
    last = None
    for attempt in range(1, retries + 1):
        try:
            if stdout_to_null:
                result = subprocess.run(
                    cmd,
                    text=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    timeout=timeout
                )
            else:
                result = subprocess.run(
                    cmd,
                    text=True,
                    capture_output=capture_output,
                    timeout=timeout
                )

            if result.returncode == 0:
                return result

            last = result
            if attempt < retries:
                delay = base_delay * attempt + random.random() * jitter
                print(f"⚠️ Command failed, retrying in {delay:.1f}s ({attempt}/{retries}): {' '.join(cmd)}")
                time.sleep(delay)

        except subprocess.TimeoutExpired as e:
            last = e
            if attempt < retries:
                delay = base_delay * attempt + random.random() * jitter
                print(f"⚠️ Command timed out, retrying in {delay:.1f}s ({attempt}/{retries}): {' '.join(cmd)}")
                time.sleep(delay)

    return last


# ------------------------------ Kaggle CLI wrappers ------------------------------
def kaggle_download(dataset_ref: str) -> bool:
    cmd = ["kaggle", "datasets", "download", "-d", dataset_ref, "-p", RAW_DIR]
    # Do not capture stdout (Kaggle progress output on Windows can hang subprocess)
    res = run_with_retry(cmd, retries=2, base_delay=3.0, jitter=2.0, timeout=None,
                         capture_output=False, stdout_to_null=True)
    return hasattr(res, "returncode") and res.returncode == 0


def kaggle_list_datasets(keyword: str, page: int):
    cmd = ["kaggle", "datasets", "list", "-s", keyword, "-p", str(page), "-v"]
    res = run_with_retry(cmd, retries=3, base_delay=2.0, jitter=1.5, timeout=90,
                         capture_output=True, stdout_to_null=False)
    if not hasattr(res, "returncode") or res.returncode != 0:
        return None
    return res.stdout


def kaggle_dataset_files(dataset_ref: str):
    cmd = ["kaggle", "datasets", "files", "-d", dataset_ref]
    res = run_with_retry(cmd, retries=3, base_delay=2.0, jitter=1.5, timeout=90,
                         capture_output=True, stdout_to_null=False)
    if not hasattr(res, "returncode") or res.returncode != 0:
        print("❌ Kaggle 'files' failed:", dataset_ref)
        try:
            print(res.stderr)
        except:
            pass
        return None
    return res.stdout


def dataset_total_size_mb_via_metadata(dataset_ref: str) -> float:
    meta_dir = os.path.join(RAW_DIR, "_meta")
    os.makedirs(meta_dir, exist_ok=True)

    # Remove stale metadata json files to avoid reading the wrong one
    for f in os.listdir(meta_dir):
        if f.endswith(".json"):
            try:
                os.remove(os.path.join(meta_dir, f))
            except:
                pass

    cmd = ["kaggle", "datasets", "metadata", "-d", dataset_ref, "-p", meta_dir]
    res = run_with_retry(cmd, retries=3, base_delay=2.0, jitter=1.5, timeout=90,
                         capture_output=True, stdout_to_null=False)
    if not hasattr(res, "returncode") or res.returncode != 0:
        try:
            print("❌ Kaggle 'metadata' failed:", dataset_ref)
            print(res.stderr)
        except:
            pass
        return -1.0

    json_files = [x for x in os.listdir(meta_dir) if x.endswith(".json")]
    if not json_files:
        return -1.0

    meta_path = os.path.join(meta_dir, json_files[0])
    try:
        import json
        with open(meta_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        total_bytes = data.get("totalBytes", None)
        if total_bytes is None:
            return -1.0
        return float(total_bytes) / (1024 * 1024)
    except Exception:
        return -1.0


# ------------------------------ Pre-check: dataset total size ------------------------------
def dataset_total_size_mb(dataset_ref: str) -> float:
    mb = dataset_total_size_mb_via_metadata(dataset_ref)
    if mb >= 0:
        return mb

    out = kaggle_dataset_files(dataset_ref)
    if out is None:
        return float("inf")

    total_mb = 0.0
    found = 0
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.lower().startswith("name") or line.startswith("-"):
            continue

        m = re.search(r"(\d+(?:\.\d+)?)\s*(KB|MB|GB)\b\s*$", line, re.IGNORECASE)
        if not m:
            continue

        num = float(m.group(1))
        unit = m.group(2).upper()
        found += 1

        if unit == "KB":
            total_mb += num / 1024
        elif unit == "MB":
            total_mb += num
        elif unit == "GB":
            total_mb += num * 1024

    if found == 0:
        return float("inf")
    return total_mb


# ------------------------------ CSV handling: rows/cols/dedup/index ------------------------------
def file_hash(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def file_size_kb(path: str) -> float:
    return round(os.path.getsize(path) / 1024, 2)


def count_rows_cols(path: str):
    with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.reader(f)
        header = next(r, [])
        cols = len(header)
        rows = 0
        for _ in r:
            rows += 1
    return rows, cols


def safe_unique_name(desired_name: str) -> str:
    p = os.path.join(CSV_DIR, desired_name)
    if not os.path.exists(p):
        return desired_name
    base, ext = os.path.splitext(desired_name)
    return f"{base}_{time.time_ns()}{ext}"


def name_signature(filename: str) -> str:
    """
    "Table name" normalized from filename (sanitized first to avoid garbled instability).
    Example: train_1.csv / train_2.csv / train003.csv => train
    """
    base = sanitize_filename(os.path.basename(filename))
    stem = os.path.splitext(base)[0].strip().lower()
    stem = re.sub(r"[\s_\-]*\(\d+\)$", "", stem)
    stem = re.sub(r"[\s_\-]*\d+$", "", stem)
    stem = " ".join(stem.split())
    return stem or stem


def write_index():
    if not index_rows:
        return
    write_header = not os.path.exists(INDEX_PATH)
    with open(INDEX_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            # ✅ Added two columns: orig_zip_name / fixed_zip_name
            w.writerow(["filename", "rows", "cols", "size_kb", "md5", "source", "keyword",
                        "name_sig", "orig_zip_name", "fixed_zip_name"])
        w.writerows(index_rows)
    index_rows.clear()


def newest_zip_in_dir(folder: str):
    zips = [f for f in os.listdir(folder) if f.endswith(".zip")]
    if not zips:
        return None
    zips.sort(key=lambda x: os.path.getmtime(os.path.join(folder, x)), reverse=True)
    return os.path.join(folder, zips[0])


def clear_raw_zips():
    # Remove leftover ZIPs before downloading a new dataset to avoid picking an old ZIP by mistake
    for f in os.listdir(RAW_DIR):
        if f.endswith(".zip"):
            try:
                os.remove(os.path.join(RAW_DIR, f))
            except:
                pass


def extract_and_filter(zip_path, dataset_ref, keyword):
    """
    Select CSVs from the dataset ZIP:
      - Filter by rows/cols
      - Global MD5 dedup
      - Max 5 CSVs per dataset
      - Prefer table-name diversity (different name_sig)
      - Handle garbled filenames: save with safe filename + store original/fixed names in index
    """
    if len(csv_hashes) >= TARGET_MAX:
        return 0

    scanned = 0
    candidates_by_name = defaultdict(list)
    all_candidates = []

    def add_candidate(tmp_path, orig_zip_name, fixed_zip_name, rows, cols, md5, sig):
        if len(candidates_by_name[sig]) >= 20:
            try:
                os.remove(tmp_path)
            except:
                pass
            return

        cand = {
            "tmp_path": tmp_path,
            "orig_zip_name": orig_zip_name,
            "fixed_zip_name": fixed_zip_name,
            "basename": os.path.basename(fixed_zip_name),
            "rows": rows,
            "cols": cols,
            "md5": md5,
            "sig": sig
        }
        candidates_by_name[sig].append(cand)
        all_candidates.append(cand)

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            for orig_zip_name in zf.namelist():
                if len(csv_hashes) >= TARGET_MAX:
                    break
                if scanned >= MAX_SCAN_CSV_ENTRIES_PER_DATASET:
                    break
                if not orig_zip_name.lower().endswith(".csv"):
                    continue

                scanned += 1

                fixed_zip_name = try_fix_zip_name(orig_zip_name)
                base = os.path.basename(fixed_zip_name)
                if not base:
                    continue

                tmp_path = os.path.join(CSV_DIR, f"_tmp_{time.time_ns()}.csv")
                try:
                    # Extract content using orig_zip_name (the real entry name)
                    with zf.open(orig_zip_name) as src, open(tmp_path, "wb") as dst:
                        dst.write(src.read())
                except Exception:
                    try:
                        os.remove(tmp_path)
                    except:
                        pass
                    continue

                try:
                    rows, cols = count_rows_cols(tmp_path)
                except Exception:
                    os.remove(tmp_path)
                    continue

                if rows < MIN_ROWS or rows > MAX_ROWS or cols < MIN_COLS:
                    os.remove(tmp_path)
                    continue

                md5 = file_hash(tmp_path)
                if md5 in csv_hashes:
                    os.remove(tmp_path)
                    continue

                sig = name_signature(base)
                add_candidate(tmp_path, orig_zip_name, fixed_zip_name, rows, cols, md5, sig)

                if len(candidates_by_name) >= MAX_CSV_PER_DATASET and len(all_candidates) >= MAX_CSV_PER_DATASET * 2:
                    break

        # Selection: prefer different table names
        selected = []
        selected_md5 = set()

        sigs = list(candidates_by_name.keys())
        sigs.sort(key=lambda s: len(candidates_by_name[s]), reverse=True)

        for sig in sigs:
            if len(selected) >= MAX_CSV_PER_DATASET:
                break
            cand = max(candidates_by_name[sig], key=lambda c: c["rows"])
            if cand["md5"] in selected_md5:
                continue
            selected.append(cand)
            selected_md5.add(cand["md5"])

        if len(selected) < MAX_CSV_PER_DATASET:
            remaining = sorted(all_candidates, key=lambda c: c["rows"], reverse=True)
            for cand in remaining:
                if len(selected) >= MAX_CSV_PER_DATASET:
                    break
                if cand["md5"] in selected_md5:
                    continue
                selected.append(cand)
                selected_md5.add(cand["md5"])

        selected_tmp = set(c["tmp_path"] for c in selected)

        # Save with safe output filenames
        added = 0
        for cand in selected:
            if len(csv_hashes) >= TARGET_MAX:
                try:
                    os.remove(cand["tmp_path"])
                except:
                    pass
                continue

            safe_name = safe_output_name(cand["basename"], cand["md5"])
            final_name = safe_unique_name(safe_name)
            final_path = os.path.join(CSV_DIR, final_name)

            try:
                os.rename(cand["tmp_path"], final_path)
            except Exception:
                try:
                    os.remove(cand["tmp_path"])
                except:
                    pass
                continue

            csv_hashes.add(cand["md5"])
            index_rows.append([
                final_name,
                cand["rows"],
                cand["cols"],
                file_size_kb(final_path),
                cand["md5"],
                dataset_ref,
                keyword,
                cand["sig"],
                cand["orig_zip_name"],
                cand["fixed_zip_name"],
            ])
            added += 1

        # Cleanup unselected temp files
        for cand in all_candidates:
            if cand["tmp_path"] not in selected_tmp:
                try:
                    os.remove(cand["tmp_path"])
                except:
                    pass

        return added

    except Exception as e:
        print("❌ Unzip/filter failed:", zip_path, e)
        for cand in all_candidates:
            try:
                os.remove(cand["tmp_path"])
            except:
                pass
        return 0


# ------------------------------ Main workflow ------------------------------
def main():
    print("===== Kaggle CSV Pipeline FINAL (retry / 2GB cap / ≤5 CSV per dataset) =====")
    print("Output directory:", BASE_DIR)
    print(f"Constraints: dataset<= {MAX_DATASET_TOTAL_MB}MB | rows {MIN_ROWS}-{MAX_ROWS} | cols>={MIN_COLS} | per-dataset<= {MAX_CSV_PER_DATASET}")

    for kw in SEARCH_KEYWORDS:
        for page in range(1, PAGES_PER_KEYWORD + 1):
            if len(csv_hashes) >= TARGET_MAX:
                break

            print(f"\n🔍 Search [{kw}] page {page}")
            out = kaggle_list_datasets(kw, page)
            if out is None:
                print("❌ Search failed (rate limit / network). Skipping this page.")
                continue

            lines = out.splitlines()
            if len(lines) < 3:
                continue

            for line in lines[2:]:
                if len(csv_hashes) >= TARGET_MAX:
                    break

                line = line.strip()
                if not line:
                    continue

                ref = line.split(",")[0].strip()
                if "/" not in ref or ref in downloaded_datasets:
                    continue

                print("📏 Checking size:", ref)
                total_mb = dataset_total_size_mb(ref)

                if total_mb == float("inf"):
                    if ALLOW_DOWNLOAD_IF_SIZE_UNKNOWN:
                        print("⚠️ Size unknown: allowed to download; will apply 2GB check on ZIP after download.")
                        total_mb = -1.0
                    else:
                        print("⏭️ Skip (unable to fetch file list/size)")
                        continue

                if total_mb > MAX_DATASET_TOTAL_MB:
                    print(f"⏭️ Skip ({total_mb:.1f} MB > {MAX_DATASET_TOTAL_MB} MB)")
                    continue

                clear_raw_zips()

                print(f"⬇️ Download ({total_mb:.1f} MB):", ref)
                if not kaggle_download(ref):
                    print("⏭️ Download failed. Skipping.")
                    continue

                downloaded_datasets.add(ref)

                zip_path = newest_zip_in_dir(RAW_DIR)
                if not zip_path:
                    print("⚠️ ZIP not found (download may have been rejected/failed).")
                    continue

                zip_mb = os.path.getsize(zip_path) / (1024 * 1024)
                if zip_mb > MAX_DATASET_TOTAL_MB:
                    print(f"⏭️ ZIP too large, deleting and skipping ({zip_mb:.1f} MB > {MAX_DATASET_TOTAL_MB} MB)")
                    try:
                        os.remove(zip_path)
                    except:
                        pass
                    continue

                added = extract_and_filter(zip_path, ref, kw)
                print(f"  ➜ Added CSVs: {added} | Total so far: {len(csv_hashes)}")

                try:
                    os.remove(zip_path)
                except:
                    pass

                write_index()
                time.sleep(BASE_SLEEP + random.random() * (JITTER_SLEEP[1] - JITTER_SLEEP[0]))

    write_index()

    if os.path.exists(RAW_DIR):
        try:
            shutil.rmtree(RAW_DIR)
            print("\n🧹 Cleaned up temporary directory: raw_datasets")
        except Exception as e:
            print("\n⚠️ Failed to remove raw_datasets:", e)

    print("\n===== Pipeline completed =====")
    print("Final CSV count:", len(csv_hashes))
    print("CSV directory:", CSV_DIR)
    print("Index file:", INDEX_PATH)


if __name__ == "__main__":
    main()
