#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
download_dimensional_history.py

用途：
  批量下载 Dimensional ETF 历史持仓 CSV。

URL 格式示例：
  https://tools-blob.dimensional.com/etf/20260504/DFAS.csv    DFAS

下载逻辑：
  - 读取 dimen_urls.txt 中的 URL + ticker
  - 把 URL 路径中的 YYYYMMDD 替换为目标日期
  - 从 2023-01-03 起逐日尝试下载
  - HTTP 404 / 403 / 400 等无文件日期直接跳过并记录
  - HTTP 200 且内容是 Dimensional CSV 才保存
  - 已存在文件默认跳过，支持断点续跑

输出目录：
  <root>/data/vendors/dimensional/raw/YYYY-MM-DD/<ticker>_holdings_YYYYMMDD.csv

示例：
  python download_dimensional_history.py --root D:\ETFData\HistoricalFeatureStore --url-file D:\ETFData\HistoricalFeatureStore\config\dimen_urls.txt --start 2023-01-03 --end yesterday --workers 3 --weekdays-only

先测试：
  python download_dimensional_history.py --root D:\ETFData\HistoricalFeatureStore --url-file .\config\dimen_urls.txt --start 2023-01-03 --end 2023-01-03 --tickers DFAS --workers 1 --print-every 1
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import os
import random
import re
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse, urlunparse
from urllib.request import HTTPCookieProcessor, Request, build_opener


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)

REQUIRED_HEADER_FIELDS = {
    "date",
    "etf_ticker",
    "ticker",
    "description",
    "weight",
    "market_value",
}


@dataclass
class UrlKey:
    ticker: str
    key_url: str


@dataclass
class DownloadResult:
    date: str
    ticker: str
    status: str
    http_status: Optional[int]
    url: str
    path: str
    bytes: int
    message: str


def parse_date(value: str) -> dt.date:
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
    raise ValueError(f"日期格式错误: {value}，请用 YYYY-MM-DD 或 YYYYMMDD")


def iter_dates(start: dt.date, end: dt.date, weekdays_only: bool = False):
    if end < start:
        raise ValueError("end 不能早于 start")
    current = start
    while current <= end:
        if not weekdays_only or current.weekday() < 5:
            yield current
        current += dt.timedelta(days=1)


def read_text_with_fallback(path: Path) -> str:
    for enc in ("utf-8-sig", "utf-8", "gbk", "gb18030"):
        try:
            return path.read_text(encoding=enc)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("unknown", b"", 0, 1, f"无法识别编码: {path}")


def clean_url(raw: str) -> str:
    s = html.unescape(raw.strip())
    s = s.replace("\\/", "/")
    s = s.replace("\\u0026", "&")
    s = s.replace("&amp;", "&")
    return s


def extract_ticker_from_url(url: str) -> str:
    path = urlparse(url).path
    name = Path(path).name
    if name.lower().endswith(".csv"):
        return name[:-4].upper()
    return ""


def replace_date_in_dimensional_url(url: str, yyyymmdd: str) -> str:
    u = clean_url(url)
    parsed = urlparse(u)
    parts = parsed.path.split("/")
    replaced = False
    for i, part in enumerate(parts):
        if re.fullmatch(r"\d{8}", part):
            parts[i] = yyyymmdd
            replaced = True
            break
    if not replaced:
        # fallback: insert date before ticker csv when possible
        if parts and parts[-1].lower().endswith(".csv"):
            parts.insert(len(parts) - 1, yyyymmdd)
        else:
            raise ValueError(f"URL 路径中找不到 YYYYMMDD 日期段: {url}")
    new_path = "/".join(parts)
    return urlunparse((parsed.scheme, parsed.netloc, new_path, parsed.params, parsed.query, parsed.fragment))


def load_url_keys(path: Path) -> list[UrlKey]:
    text = read_text_with_fallback(path)
    keys: list[UrlKey] = []
    seen: set[str] = set()

    for line_no, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        parts = re.split(r"\s+", line)
        url = ""
        for part in parts:
            if part.lower().startswith(("http://", "https://")):
                url = clean_url(part)
                break
        if not url:
            continue

        ticker = ""
        if len(parts) >= 2:
            candidate = parts[-1].strip().upper()
            if re.fullmatch(r"[A-Z0-9]{1,16}", candidate):
                ticker = candidate
        if not ticker:
            ticker = extract_ticker_from_url(url)
        if not ticker:
            print(f"[WARN] 第 {line_no} 行无法识别 ticker，跳过: {raw_line}")
            continue

        key_id = f"{ticker}|{url}"
        if key_id in seen:
            continue
        seen.add(key_id)
        keys.append(UrlKey(ticker=ticker, key_url=url))

    # 同 ticker 多个 URL 时保留第一个
    deduped: list[UrlKey] = []
    seen_tickers: set[str] = set()
    for k in keys:
        if k.ticker in seen_tickers:
            continue
        seen_tickers.add(k.ticker)
        deduped.append(k)

    if not deduped:
        raise ValueError(f"没有从文件中解析到 Dimensional URL key: {path}")
    return deduped


def looks_like_html(data: bytes) -> bool:
    prefix = data[:2048].lstrip().lower()
    return (
        prefix.startswith(b"<!doctype html")
        or prefix.startswith(b"<html")
        or b"<html" in prefix[:512]
        or b"access denied" in prefix
        or b"request rejected" in prefix
        or b"not found" in prefix[:512]
    )


def validate_dimensional_csv(data: bytes, expected_ticker: str | None = None) -> tuple[bool, str]:
    if not data:
        return False, "empty body"
    if looks_like_html(data):
        return False, "body looks like html/access denied/not found"

    sample = data[:8192].decode("utf-8-sig", errors="replace")
    lines = [x.strip() for x in sample.splitlines() if x.strip()]
    if not lines:
        return False, "no non-empty lines"

    header = [x.strip().lower() for x in lines[0].split(",")]
    missing = sorted(REQUIRED_HEADER_FIELDS - set(header))
    if missing:
        return False, f"missing required header fields: {missing}"

    if len(lines) < 2:
        return False, "header only, no holding rows"

    if expected_ticker and "etf_ticker" in header:
        # 只做弱检查，避免因为样本前几行特殊导致误杀
        idx = header.index("etf_ticker")
        found = False
        for line in lines[1:20]:
            cols = line.split(",")
            if len(cols) > idx and cols[idx].strip().upper() == expected_ticker.upper():
                found = True
                break
        if not found:
            return False, f"etf_ticker not found in first rows: expected {expected_ticker}"

    return True, "ok"


def atomic_write_bytes(out_file: Path, data: bytes) -> None:
    out_file.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=out_file.name + ".", suffix=".tmp", dir=str(out_file.parent))
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp_name, out_file)
    finally:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)


def download_one(
    *,
    root: Path,
    vendor: str,
    key: UrlKey,
    date_obj: dt.date,
    timeout: int,
    retries: int,
    overwrite: bool,
    dry_run: bool,
    min_sleep: float,
    max_sleep: float,
    save_suspicious: bool,
) -> DownloadResult:
    yyyymmdd = date_obj.strftime("%Y%m%d")
    date_dash = date_obj.strftime("%Y-%m-%d")
    url = replace_date_in_dimensional_url(key.key_url, yyyymmdd)

    out_dir = root / "data" / "vendors" / vendor / "raw" / date_dash
    out_file = out_dir / f"{key.ticker.lower()}_holdings_{yyyymmdd}.csv"

    if out_file.exists() and out_file.stat().st_size > 0 and not overwrite:
        return DownloadResult(date_dash, key.ticker, "exists", None, url, str(out_file), out_file.stat().st_size, "skip existing file")

    if dry_run:
        return DownloadResult(date_dash, key.ticker, "dry_run", None, url, str(out_file), 0, "dry run only")

    if max_sleep > 0:
        time.sleep(random.uniform(min_sleep, max_sleep))

    opener = build_opener(HTTPCookieProcessor())
    last_message = ""

    for attempt in range(retries + 1):
        try:
            req = Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/csv,application/csv,text/plain,*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://www.dimensional.com/",
                    "Connection": "close",
                },
                method="GET",
            )
            with opener.open(req, timeout=timeout) as resp:
                http_status = getattr(resp, "status", None) or resp.getcode()
                data = resp.read()

            if http_status == 200:
                ok, msg = validate_dimensional_csv(data, expected_ticker=key.ticker)
                if ok:
                    atomic_write_bytes(out_file, data)
                    return DownloadResult(date_dash, key.ticker, "downloaded", http_status, url, str(out_file), len(data), "ok")

                if save_suspicious:
                    atomic_write_bytes(out_file, data)
                    return DownloadResult(date_dash, key.ticker, "suspicious_saved", http_status, url, str(out_file), len(data), msg)

                return DownloadResult(date_dash, key.ticker, "bad_content", http_status, url, str(out_file), len(data), msg)

            if http_status in (400, 403, 404):
                return DownloadResult(date_dash, key.ticker, "not_found", http_status, url, str(out_file), 0, f"HTTP {http_status}")

            return DownloadResult(date_dash, key.ticker, "http_other", http_status, url, str(out_file), 0, f"unexpected http status: {http_status}")

        except HTTPError as exc:
            if exc.code in (400, 403, 404):
                return DownloadResult(date_dash, key.ticker, "not_found", exc.code, url, str(out_file), 0, f"HTTP {exc.code}")
            last_message = f"HTTPError {exc.code}: {exc.reason}"
            if exc.code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(2.0 * (attempt + 1))
                continue
            return DownloadResult(date_dash, key.ticker, "http_error", exc.code, url, str(out_file), 0, last_message)

        except (URLError, TimeoutError, ConnectionError) as exc:
            last_message = repr(exc)
            if attempt < retries:
                time.sleep(2.0 * (attempt + 1))
                continue
            return DownloadResult(date_dash, key.ticker, "network_error", None, url, str(out_file), 0, last_message)

        except Exception as exc:
            return DownloadResult(date_dash, key.ticker, "error", None, url, str(out_file), 0, repr(exc))

    return DownloadResult(date_dash, key.ticker, "failed", None, url, str(out_file), 0, last_message or "failed after retries")


def write_log(log_path: Path, rows: list[DownloadResult]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["date", "ticker", "status", "http_status", "url", "path", "bytes", "message", "logged_at"]
    with log_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        now = dt.datetime.now().isoformat(timespec="seconds")
        for r in rows:
            writer.writerow({
                "date": r.date,
                "ticker": r.ticker,
                "status": r.status,
                "http_status": r.http_status if r.http_status is not None else "",
                "url": r.url,
                "path": r.path,
                "bytes": r.bytes,
                "message": r.message,
                "logged_at": now,
            })


def main() -> int:
    parser = argparse.ArgumentParser(description="Download Dimensional historical holdings CSV files.")
    parser.add_argument("--root", default=r"D:\ETFData\HistoricalFeatureStore", help="项目根目录")
    parser.add_argument("--vendor", default="dimensional", help="vendor 名称，默认 dimensional")
    parser.add_argument("--url-file", required=True, help="包含 Dimensional URL + ticker 的 txt")
    parser.add_argument("--start", default="2023-01-03", help="开始日期，默认 2023-01-03")
    parser.add_argument("--end", default="yesterday", help="结束日期 YYYY-MM-DD / YYYYMMDD / today / yesterday")
    parser.add_argument("--workers", type=int, default=3, help="并发下载线程数，建议 1-4")
    parser.add_argument("--timeout", type=int, default=35, help="单请求超时秒数")
    parser.add_argument("--retries", type=int, default=2, help="网络错误 / 429 / 5xx 重试次数")
    parser.add_argument("--overwrite", action="store_true", help="覆盖已存在文件")
    parser.add_argument("--dry-run", action="store_true", help="只打印计划，不下载")
    parser.add_argument("--weekdays-only", action="store_true", help="只尝试周一到周五")
    parser.add_argument("--tickers", nargs="*", default=None, help="只下载指定 ticker，用于测试，例如 --tickers DFAS DFAI")
    parser.add_argument("--min-sleep", type=float, default=0.2, help="每个请求前最小随机等待秒数")
    parser.add_argument("--max-sleep", type=float, default=1.0, help="每个请求前最大随机等待秒数")
    parser.add_argument("--save-suspicious", action="store_true", help="异常内容也保存，便于人工检查；默认不保存")
    parser.add_argument("--print-every", type=int, default=100, help="每处理多少个任务打印一次进度")

    args = parser.parse_args()
    root = Path(args.root)
    vendor = args.vendor.strip().lower()
    url_file = Path(args.url_file)
    start = parse_date(args.start)
    end = parse_date(args.end)

    keys = load_url_keys(url_file)
    if args.tickers:
        requested = {x.upper() for x in args.tickers}
        keys = [k for k in keys if k.ticker.upper() in requested]
        missing = sorted(requested - {k.ticker.upper() for k in keys})
        if missing:
            print(f"[WARN] url-file 中没找到这些 ticker: {missing}")

    if not keys:
        print("[ERROR] URL key 列表为空")
        return 2

    dates = list(iter_dates(start, end, weekdays_only=args.weekdays_only))
    total_tasks = len(keys) * len(dates)

    print("========================================")
    print("Dimensional ETF holdings history downloader")
    print("========================================")
    print(f"root          : {root}")
    print(f"vendor        : {vendor}")
    print(f"url_file      : {url_file}")
    print(f"tickers       : {len(keys)}")
    print(f"date range    : {start} -> {end}")
    print(f"dates         : {len(dates)}")
    print(f"tasks         : {total_tasks}")
    print(f"workers       : {args.workers}")
    print(f"dry_run       : {args.dry_run}")
    print(f"weekdays_only : {args.weekdays_only}")
    print("========================================")

    if total_tasks == 0:
        print("[ERROR] 没有任务")
        return 2

    results: list[DownloadResult] = []
    counters: dict[str, int] = {}
    processed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for d in dates:
            for key in keys:
                futures.append(executor.submit(
                    download_one,
                    root=root,
                    vendor=vendor,
                    key=key,
                    date_obj=d,
                    timeout=args.timeout,
                    retries=args.retries,
                    overwrite=args.overwrite,
                    dry_run=args.dry_run,
                    min_sleep=args.min_sleep,
                    max_sleep=args.max_sleep,
                    save_suspicious=args.save_suspicious,
                ))

        for fut in as_completed(futures):
            r = fut.result()
            results.append(r)
            counters[r.status] = counters.get(r.status, 0) + 1
            processed += 1
            if processed % args.print_every == 0 or processed == total_tasks:
                downloaded = counters.get("downloaded", 0)
                exists = counters.get("exists", 0)
                not_found = counters.get("not_found", 0)
                bad = counters.get("bad_content", 0)
                suspicious = counters.get("suspicious_saved", 0)
                errors = sum(v for k, v in counters.items() if k not in ("downloaded", "exists", "not_found", "dry_run", "bad_content", "suspicious_saved"))
                print(f"[{processed}/{total_tasks}] downloaded={downloaded}, exists={exists}, not_found={not_found}, bad={bad}, suspicious={suspicious}, errors={errors}")

    start_s = start.strftime("%Y%m%d")
    end_s = end.strftime("%Y%m%d")
    log_path = root / "data" / "vendors" / vendor / "history" / f"download_log_{vendor}_{start_s}_{end_s}.csv"
    write_log(log_path, results)

    print("\n========== SUMMARY ==========")
    for k in sorted(counters):
        print(f"{k:18s}: {counters[k]}")
    print(f"log: {log_path}")
    print("=============================")

    hard_errors = sum(v for k, v in counters.items() if k not in ("downloaded", "exists", "not_found", "dry_run", "bad_content", "suspicious_saved"))
    return 1 if hard_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
