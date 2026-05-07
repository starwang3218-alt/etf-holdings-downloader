#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
package_vendor_raw_by_year.py

用途：
  将 <root>/data/vendors/<vendor>/raw/YYYY-MM-DD/*.csv 按年份打包为 zip。

示例：
  python package_vendor_raw_by_year.py --root D:\ETFData\HistoricalFeatureStore --vendor dimensional --start 2023 --end 2026
"""

from __future__ import annotations

import argparse
import zipfile
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Package vendor raw CSV files by year.")
    parser.add_argument("--root", default=r"D:\ETFData\HistoricalFeatureStore", help="项目根目录")
    parser.add_argument("--vendor", default="dimensional", help="vendor 名称")
    parser.add_argument("--start", type=int, default=2023, help="开始年份")
    parser.add_argument("--end", type=int, default=2026, help="结束年份")
    parser.add_argument("--overwrite", action="store_true", help="覆盖已存在 zip")
    args = parser.parse_args()

    root = Path(args.root)
    raw_root = root / "data" / "vendors" / args.vendor / "raw"
    pkg_dir = root / "data" / "vendors" / args.vendor / "packages"
    pkg_dir.mkdir(parents=True, exist_ok=True)

    if not raw_root.exists():
        print(f"[ERROR] raw 目录不存在: {raw_root}")
        return 2

    for year in range(args.start, args.end + 1):
        files = sorted(raw_root.glob(f"{year}-??-??/*.csv"))
        zip_path = pkg_dir / f"{args.vendor}_raw_{year}.zip"

        if zip_path.exists() and not args.overwrite:
            print(f"[skip] {zip_path} exists, files={len(files)}")
            continue

        print(f"[pack] year={year}, files={len(files)}, zip={zip_path}")
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for f in files:
                arcname = f.relative_to(raw_root)
                zf.write(f, arcname.as_posix())

        print(f"[done] {zip_path} size_mb={zip_path.stat().st_size / 1024 / 1024:.2f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
