#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
cleanup_dimensional_empty_files.py

用途：
  清理 Dimensional 下载目录中的无效 CSV：空文件、HTML 错误页、只有表头没有持仓行、缺少核心字段的文件。

默认 dry-run，不删除；确认日志后加 --delete。

示例：
  python cleanup_dimensional_empty_files.py --root D:\ETFData\HistoricalFeatureStore
  python cleanup_dimensional_empty_files.py --root D:\ETFData\HistoricalFeatureStore --delete
  python cleanup_dimensional_empty_files.py --root D:\ETFData\HistoricalFeatureStore --start 2023-01-03 --end today --delete
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
from pathlib import Path

REQUIRED_HEADER_FIELDS = {"date", "etf_ticker", "ticker", "description", "weight", "market_value"}


def parse_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    v = value.strip().lower()
    if v == "today":
        return dt.date.today()
    if v == "yesterday":
        return dt.date.today() - dt.timedelta(days=1)
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return dt.datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"日期格式错误: {value}")


def parse_date_from_dir_name(name: str) -> dt.date | None:
    try:
        return dt.datetime.strptime(name, "%Y-%m-%d").date()
    except ValueError:
        return None


def read_text(path: Path) -> str:
    raw = path.read_bytes()
    for enc in ("utf-8-sig", "utf-8", "latin-1", "gb18030"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def is_bad_dimensional_csv(path: Path) -> tuple[bool, str]:
    try:
        size = path.stat().st_size
        if size == 0:
            return True, "empty_file"
        text = read_text(path)
    except Exception as exc:
        return False, f"read_error: {exc!r}"

    head = text[:4096].lstrip().lower()
    if head.startswith("<!doctype html") or head.startswith("<html") or "<html" in head[:512]:
        return True, "html_error_page"
    if "access denied" in head[:1024] or "request rejected" in head[:1024]:
        return True, "access_denied_page"

    lines = [x.strip() for x in text.splitlines() if x.strip()]
    if not lines:
        return True, "no_non_empty_lines"

    header = [x.strip().lower() for x in lines[0].split(",")]
    missing = sorted(REQUIRED_HEADER_FIELDS - set(header))
    if missing:
        return True, f"missing_required_header_fields={missing}"

    if len(lines) < 2:
        return True, "header_only_no_rows"

    # 至少要有一行像持仓行，且列数不少于核心字段数。
    possible_rows = 0
    for line in lines[1:20]:
        if line.count(",") >= len(REQUIRED_HEADER_FIELDS) - 1:
            possible_rows += 1
    if possible_rows == 0:
        return True, "no_possible_holding_rows"

    return False, f"ok_possible_rows={possible_rows}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Cleanup invalid Dimensional holdings CSV files.")
    parser.add_argument("--root", default=r"D:\ETFData\HistoricalFeatureStore", help="项目根目录")
    parser.add_argument("--vendor", default="dimensional", help="vendor 名称，默认 dimensional")
    parser.add_argument("--start", default=None, help="开始日期 YYYY-MM-DD，可选")
    parser.add_argument("--end", default=None, help="结束日期 YYYY-MM-DD/today/yesterday，可选")
    parser.add_argument("--delete", action="store_true", help="真正删除；不加则只 dry-run")
    parser.add_argument("--print-every", type=int, default=1000, help="每扫描多少个文件打印一次进度")
    args = parser.parse_args()

    root = Path(args.root)
    raw_root = root / "data" / "vendors" / args.vendor / "raw"
    history_dir = root / "data" / "vendors" / args.vendor / "history"
    history_dir.mkdir(parents=True, exist_ok=True)

    start = parse_date(args.start)
    end = parse_date(args.end)

    if not raw_root.exists():
        print(f"[ERROR] raw 目录不存在: {raw_root}")
        return 2

    rows = []
    scanned = matched = deleted = 0
    date_dirs = sorted([p for p in raw_root.iterdir() if p.is_dir()], key=lambda p: p.name)

    for date_dir in date_dirs:
        date_obj = parse_date_from_dir_name(date_dir.name)
        if date_obj is None:
            continue
        if start and date_obj < start:
            continue
        if end and date_obj > end:
            continue

        for file_path in date_dir.glob("*.csv"):
            scanned += 1
            if scanned % args.print_every == 0:
                print(f"[scan] scanned={scanned}, matched={matched}, deleted={deleted}")

            is_bad, reason = is_bad_dimensional_csv(file_path)
            if is_bad:
                matched += 1
                action = "dry_run"
                if args.delete:
                    file_path.unlink()
                    deleted += 1
                    action = "deleted"
                rows.append({
                    "date": date_dir.name,
                    "file": str(file_path),
                    "bytes": file_path.stat().st_size if file_path.exists() else 0,
                    "reason": reason,
                    "action": action,
                })

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = history_dir / f"cleanup_invalid_dimensional_{ts}.csv"
    with log_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "file", "bytes", "reason", "action"])
        writer.writeheader()
        writer.writerows(rows)

    print("========================================")
    print("Dimensional invalid CSV cleanup")
    print("========================================")
    print(f"raw_root   : {raw_root}")
    print(f"date range : {start or '-'} -> {end or '-'}")
    print(f"mode       : {'DELETE' if args.delete else 'DRY-RUN'}")
    print(f"scanned    : {scanned}")
    print(f"matched    : {matched}")
    print(f"deleted    : {deleted}")
    print(f"log        : {log_path}")
    print("========================================")
    if not args.delete:
        print("[NOTE] 当前只是 dry-run，没有删除。确认日志没问题后，加 --delete 再跑。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
