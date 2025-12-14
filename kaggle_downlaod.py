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

# ================== 基础配置==================
KAGGLE_API_TOKEN = ""
os.environ["KAGGLE_API_TOKEN"] = KAGGLE_API_TOKEN

BASE_DIR = r"D:\kaggle_pool"   # 全部输出放到 D 盘这个目录
# ===============================================================

# ===================== 采集目标与过滤参数 ======================
TARGET_MAX = 8000
ALLOW_DOWNLOAD_IF_SIZE_UNKNOWN = True

MIN_ROWS = 300
MAX_ROWS = 50000
MIN_COLS = 4

MAX_CSV_PER_DATASET = 5                   # 每个数据集最终最多落盘 5 个
MAX_SCAN_CSV_ENTRIES_PER_DATASET = 200    # 每个数据集最多扫描多少个CSV条目（够用且快）
MAX_DATASET_TOTAL_MB = 2048               # 下载前预检查：数据集总大小 <= 2GB 才下载

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

# 每个数据集之间的基础休眠（会叠加随机抖动，降低被限流概率）
BASE_SLEEP = 0.6
JITTER_SLEEP = (0.0, 0.6)  # 随机加 0~0.6 秒
# ===============================================================

RAW_DIR = os.path.join(BASE_DIR, "raw_datasets")
CSV_DIR = os.path.join(BASE_DIR, "all_csv")
INDEX_PATH = os.path.join(BASE_DIR, "index.csv")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)

csv_hashes = set()            # 全局去重：内容 MD5
downloaded_datasets = set()
index_rows = []


# ------------------------------ 文件名乱码处理（新增） ------------------------------
def try_fix_zip_name(name: str) -> str:
    """
    尝试修复 zip 内部文件名编码（不保证100%）。
    常见情况：zip按cp437解释，实际是utf-8/gbk/big5。
    """
    # 如果本来就没有替换符，直接返回
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
        # 选替换符最少的那个
        candidates.sort(key=lambda s: s.count("�"))
        best = candidates[0]
        # 如果没有更好，就别乱改
        if best.count("�") >= name.count("�"):
            return name
        return best
    except Exception:
        return name


def sanitize_filename(name: str, max_len: int = 120) -> str:
    """
    把任意字符串变成 Windows 可落盘的文件名：
      - 规范化 Unicode
      - 去掉 Windows 禁止字符
      - 非常规字符替换成 _
      - 压缩多余空格/下划线
      - 截断长度
    """
    name = unicodedata.normalize("NFKC", name)

    # 去掉 Windows 不允许的字符和控制字符
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", name)

    # 只保留安全字符集合，其余替换 _
    name = re.sub(r'[^0-9a-zA-Z._\- \u4e00-\u9fff]+', "_", name)

    # 压缩空白/下划线
    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"_+", "_", name)

    # 截断
    if len(name) > max_len:
        base, ext = os.path.splitext(name)
        name = base[: max_len - len(ext)] + ext

    return name or "file"


def safe_output_name(orig_basename: str, md5_hex: str) -> str:
    """
    用“原始 basename（可能乱码）+ md5短后缀”生成安全文件名，避免冲突/乱码。
    """
    base, ext = os.path.splitext(orig_basename)
    ext = ext if ext else ".csv"
    safe_base = sanitize_filename(base)
    suffix = md5_hex[:10]
    return f"{safe_base}_{suffix}{ext}"


# ------------------------------ 通用工具：重试执行 ------------------------------
def run_with_retry(cmd, *, retries=3, base_delay=2.0, jitter=1.0, timeout=None,
                   capture_output=False, stdout_to_null=False):
    """
    用于 kaggle list/files 这类“偶发网络失败”的命令。
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
                print(f"⚠️ 命令失败，{delay:.1f}s 后重试 ({attempt}/{retries})：{' '.join(cmd)}")
                time.sleep(delay)

        except subprocess.TimeoutExpired as e:
            last = e
            if attempt < retries:
                delay = base_delay * attempt + random.random() * jitter
                print(f"⚠️ 命令超时，{delay:.1f}s 后重试 ({attempt}/{retries})：{' '.join(cmd)}")
                time.sleep(delay)

    return last


# ------------------------------ Kaggle命令封装 ------------------------------
def kaggle_download(dataset_ref: str) -> bool:
    cmd = ["kaggle", "datasets", "download", "-d", dataset_ref, "-p", RAW_DIR]
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
        print("❌ files失败:", dataset_ref)
        try:
            print(res.stderr)
        except:
            pass
        return None
    return res.stdout


def dataset_total_size_mb_via_metadata(dataset_ref: str) -> float:
    meta_dir = os.path.join(RAW_DIR, "_meta")
    os.makedirs(meta_dir, exist_ok=True)

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
            print("❌ metadata失败:", dataset_ref)
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


# ------------------------------ 预检查：数据集大小 ------------------------------
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


# ------------------------------ CSV 处理：行列/去重/索引 ------------------------------
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
    “表名”：按文件名归一化（但先 sanitize，避免乱码导致不稳定）。
    例：train_1.csv / train_2.csv / train003.csv => train
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
            # ✅ 新增两列：orig_zip_name / fixed_zip_name
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
    for f in os.listdir(RAW_DIR):
        if f.endswith(".zip"):
            try:
                os.remove(os.path.join(RAW_DIR, f))
            except:
                pass


def extract_and_filter(zip_path, dataset_ref, keyword):
    """
    从zip里挑CSV：
      - 行列过滤
      - MD5全局去重
      - 每数据集最多落盘 5 个
      - 表名多样性优先（name_sig 不同优先）
      - 文件名乱码：落盘统一安全文件名 + index记录原始/修复名
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
                    # 解压内容仍然用 orig_zip_name（真实存在的条目）
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

        # 选择：优先不同表名
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

        # 统一安全文件名
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

        # 清理未选中的临时文件
        for cand in all_candidates:
            if cand["tmp_path"] not in selected_tmp:
                try:
                    os.remove(cand["tmp_path"])
                except:
                    pass

        return added

    except Exception as e:
        print("❌ 解压/筛选失败:", zip_path, e)
        for cand in all_candidates:
            try:
                os.remove(cand["tmp_path"])
            except:
                pass
        return 0


# ------------------------------ 主流程 ------------------------------
def main():
    print("===== Kaggle CSV Pipeline FINAL（带重试/限2GB/每数据集≤5 CSV）=====")
    print("输出目录：", BASE_DIR)
    print(f"限制：dataset<= {MAX_DATASET_TOTAL_MB}MB | rows {MIN_ROWS}-{MAX_ROWS} | cols>={MIN_COLS} | per-dataset<= {MAX_CSV_PER_DATASET}")

    for kw in SEARCH_KEYWORDS:
        for page in range(1, PAGES_PER_KEYWORD + 1):
            if len(csv_hashes) >= TARGET_MAX:
                break

            print(f"\n🔍 搜索 [{kw}] 第 {page} 页")
            out = kaggle_list_datasets(kw, page)
            if out is None:
                print("❌ 搜索失败（可能限流/网络波动），跳过这一页")
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

                print("📏 检查大小:", ref)
                total_mb = dataset_total_size_mb(ref)

                if total_mb == float("inf"):
                    if ALLOW_DOWNLOAD_IF_SIZE_UNKNOWN:
                        print("⚠️ 大小未知：允许下载，下载后再按zip大小做2GB过滤")
                        total_mb = -1.0
                    else:
                        print("⏭️ 跳过（无法获取文件列表/大小）")
                        continue

                if total_mb > MAX_DATASET_TOTAL_MB:
                    print(f"⏭️ 跳过（{total_mb:.1f} MB > {MAX_DATASET_TOTAL_MB} MB）")
                    continue

                clear_raw_zips()

                print(f"⬇️ 下载 ({total_mb:.1f} MB):", ref)
                if not kaggle_download(ref):
                    print("⏭️ 下载失败，跳过")
                    continue

                downloaded_datasets.add(ref)

                zip_path = newest_zip_in_dir(RAW_DIR)
                if not zip_path:
                    print("⚠️ 没找到 zip（可能下载被拒绝/失败）")
                    continue

                zip_mb = os.path.getsize(zip_path) / (1024 * 1024)
                if zip_mb > MAX_DATASET_TOTAL_MB:
                    print(f"⏭️ zip太大，删除并跳过（{zip_mb:.1f} MB > {MAX_DATASET_TOTAL_MB} MB）")
                    try:
                        os.remove(zip_path)
                    except:
                        pass
                    continue

                added = extract_and_filter(zip_path, ref, kw)
                print(f"  ➜ 新增 CSV: {added} | 当前总数: {len(csv_hashes)}")

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
            print("\n🧹 已清理临时目录 raw_datasets")
        except Exception as e:
            print("\n⚠️ 清理 raw_datasets 失败:", e)

    print("\n===== Pipeline 完成 =====")
    print("最终 CSV 数量:", len(csv_hashes))
    print("CSV 目录:", CSV_DIR)
    print("索引文件:", INDEX_PATH)


if __name__ == "__main__":
    main()
