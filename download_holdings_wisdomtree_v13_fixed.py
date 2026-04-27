#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import re
import random
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# 【升级1：使用更新、更常见的指纹】
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

BLOCK_HINTS = [
    "access denied",
    "akamai",
    "forbidden",
    "temporarily unavailable",
    "request unsuccessful",
    "bot",
    "captcha",
    "cloudflare"
]

@dataclass
class Job:
    url: str
    name: str
    original_url: str = ""

@dataclass
class DownloadResult:
    ok: bool
    page_url: str
    page_name: str
    saved_path: str = ""
    file_url: str = ""
    via: str = ""
    note: str = ""

def safe_name(text: str, max_len: int = 120) -> str:
    text = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", str(text))
    text = re.sub(r"\s+", " ", text).strip().rstrip(".")
    return text[:max_len].strip() or "fund"

def parse_input_line(line: str) -> Job | None:
    raw = line.strip()
    if not raw or raw.startswith("#"):
        return None
    parts = [p.strip() for p in re.split(r"\t+|\s{2,}", raw) if p.strip()]
    if not parts:
        return None
    if parts[0].startswith("http://") or parts[0].startswith("https://"):
        url = parts[0]
        name = parts[-1] if len(parts) >= 2 else Path(url).name
        return Job(name=name.upper(), url=url, original_url=url)
    return None

def parse_jobs(input_file: Path) -> list[Job]:
    jobs = []
    for line in input_file.read_text(encoding="utf-8").splitlines():
        job = parse_input_line(line)
        if job:
            jobs.append(job)
    return jobs

def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8",
        }
    )
    return s

async def save_debug(page, output_dir: Path, name: str, prefix: str) -> None:
    debug_dir = output_dir.parent / "_debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    stem = safe_name(name)
    try:
        html = await page.content()
        (debug_dir / f"{stem}_{prefix}.html").write_text(html, encoding="utf-8")
        text = await page.text_content("body") or ""
        (debug_dir / f"{stem}_{prefix}.txt").write_text(text, encoding="utf-8")
        await page.screenshot(path=str(debug_dir / f"{stem}_{prefix}.png"), full_page=True)
    except Exception:
        pass

async def find_all_holdings_modal_url(page) -> str:
    hrefs = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(a => a.href || a.getAttribute('href') || '').filter(Boolean)"
    )
    patterns = [re.compile(r"all-holdings", re.I), re.compile(r"holdings", re.I)]
    for href in hrefs:
        h = href.lower()
        if "wisdomtree.com" not in h: continue
        if any(p.search(h) for p in patterns): return href
    html = await page.content()
    regexes = [
        r'https://www\.wisdomtree\.com/[^"\']*all-holdings[^"\']*',
        r'https://www\.wisdomtree\.com/[^"\']*holdings[^"\']*',
        r'(/global/etf-details/modals/all-holdings\?id=[^"\']+)',
        r'(/global/etf-details/modals/[^"\']*holdings[^"\']*)',
    ]
    for pat in regexes:
        m = re.search(pat, html, re.I)
        if m: return urljoin(page.url, m.group(1) if m.lastindex else m.group(0))
    return ""

async def open_view_all_holdings_inline(page) -> bool:
    patterns = [re.compile(r"View All Holdings", re.I), re.compile(r"All Holdings", re.I), re.compile(r"Holdings", re.I)]
    for pat in patterns:
        locators = [page.get_by_role("button", name=pat), page.get_by_role("link", name=pat), page.get_by_text(pat)]
        for locator in locators:
            try:
                if await locator.first.count() == 0: continue
                try: await locator.first.click(timeout=6000)
                except: await locator.first.click(timeout=6000, force=True)
                await page.wait_for_timeout(3000)
                return True
            except: continue
    return False

def extract_as_of_from_text(text: str) -> str:
    if not text: return ""
    lines = [re.sub(r"\s+", " ", x).strip() for x in text.splitlines() if x.strip()]
    for line in lines[:80]:
        m = re.search(r"As of\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", line, re.I)
        if m: return m.group(1).strip()
    m = re.search(r"As of\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", text, re.I)
    return m.group(1).strip() if m else ""

def normalize_header_name(name: str) -> str:
    s = re.sub(r"\s+", " ", str(name or "")).strip().lower()
    mapping = {
        "security name": "Security_Name", "holding ticker": "Holding_Ticker", "ticker": "Holding_Ticker",
        "identifier": "Identifier", "country": "Country", "quantity": "Quantity", "shares": "Quantity",
        "weight": "Weight", "market value": "Market_Value", "asset class": "Asset_Class",
        "sedol": "SEDOL", "cusip": "CUSIP", "isin": "ISIN", "figi": "FIGI", "security type": "Security_Type"
    }
    return mapping.get(s, re.sub(r"[^0-9A-Za-z]+", "_", s).strip("_") or "col")

def flatten_columns(cols) -> list[str]:
    if not isinstance(cols, pd.MultiIndex): return [str(c).strip() for c in cols]
    out = []
    for tup in cols:
        parts = [str(x).strip() for x in tup if str(x).strip() and str(x).lower() != "nan"]
        out.append(parts[-1] if parts else "")
    return out

def parse_tables_safely(html: str) -> list[pd.DataFrame]:
    all_dfs, seen_signatures = [], set()
    for header in ([0, 1, 2], [0, 1], 0, None):
        try: dfs = pd.read_html(StringIO(html), header=header)
        except: continue
        for df in dfs:
            try:
                df = df.copy()
                df.columns = flatten_columns(df.columns)
                df.columns = [normalize_header_name(c) for c in df.columns]
                df = df.dropna(how="all")
                if df.empty: continue
                signature = (tuple(df.columns), len(df))
                if signature in seen_signatures: continue
                seen_signatures.add(signature)
                all_dfs.append(df)
            except: continue
    return all_dfs

def choose_best_table(dfs: list[pd.DataFrame]) -> pd.DataFrame | None:
    keywords = ["security_name", "holding_ticker", "ticker", "figi", "identifier", "country", "quantity", "weight", "market_value"]
    scored = []
    for df in dfs:
        try:
            probe = df.copy()
            probe.columns = [str(c).strip() for c in probe.columns]
            probe = probe.dropna(how="all")
            if probe.empty: continue
            cols = " | ".join([str(c).lower() for c in probe.columns])
            score = len(probe) + len(probe.columns) * 10
            score += sum(100 for kw in keywords if kw in cols)
            if len(probe) >= 20: score += 100
            if "weight" in cols: score += 80
            scored.append((score, probe))
        except: continue
    if not scored: return None
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]

def clean_table(df: pd.DataFrame, task: Job, modal_url: str, as_of: str) -> pd.DataFrame:
    out = df.copy()
    out.columns = [normalize_header_name(c) for c in out.columns]
    for col in ["Holding_Ticker", "Security_Name"]:
        if col in out.columns:
            out = out[~out[col].astype(str).str.strip().str.lower().isin(["ticker", "holding ticker", "security name"])]
    out = out.dropna(axis=1, how="all").dropna(axis=0, how="all").reset_index(drop=True)
    out.insert(0, "ETF_Ticker", task.name)
    out.insert(1, "Record_Date", as_of.replace("-", "/") if as_of else "")
    out.insert(2, "Source_URL", task.url)
    out.insert(3, "Modal_URL", modal_url)
    return out

def looks_blocked(text: str) -> bool:
    t = (text or "").lower()
    return any(hint in t for hint in BLOCK_HINTS)

async def try_export_holdings(page, output_dir: Path, name: str) -> Path | None:
    patterns = [re.compile(r"Export Holdings", re.I), re.compile(r"Export", re.I), re.compile(r"Download", re.I)]
    for pat in patterns:
        locators = [page.get_by_role("link", name=pat), page.get_by_role("button", name=pat), page.get_by_text(pat)]
        for locator in locators:
            try:
                if await locator.first.count() == 0: continue
                for _ in range(2):
                    try:
                        async with page.expect_download(timeout=12000) as download_info:
                            try: await locator.first.click(timeout=5000)
                            except: await locator.first.click(timeout=5000, force=True)
                        download = await download_info.value
                        suggested = download.suggested_filename or f"{safe_name(name)}.csv"
                        if not suggested.lower().endswith((".csv", ".xlsx", ".xls")): suggested = f"{Path(suggested).stem}.csv"
                        save_path = output_dir.parent / "_tmp" / suggested
                        save_path.parent.mkdir(parents=True, exist_ok=True)
                        await download.save_as(str(save_path))
                        return save_path
                    except: await page.wait_for_timeout(1500)
            except: continue
    return None

async def try_parse_and_save(page, task: Job, output_dir: Path, csv_path: Path, modal_url: str) -> DownloadResult | None:
    body_text = await page.text_content("body") or ""
    as_of = extract_as_of_from_text(body_text)

    export_path = await try_export_holdings(page, output_dir, task.name)
    if export_path and export_path.exists():
        try:
            if export_path.suffix.lower() == ".csv":
                try: df = pd.read_csv(export_path, dtype=str)
                except: df = pd.read_csv(export_path, dtype=str, encoding="latin1")
            else:
                last_err, df = None, None
                for engine in [None, "openpyxl", "xlrd"]:
                    try: df = pd.read_excel(export_path, dtype=str, engine=engine); break
                    except Exception as e: last_err = e
                if df is None: raise RuntimeError(str(last_err))
            df = clean_table(df, task, modal_url, as_of)
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
            export_path.unlink(missing_ok=True)
            return DownloadResult(True, task.url, task.name, str(csv_path), modal_url, "wisdomtree-export", f"账期: {as_of}")
        except: pass

    html = await page.content()
    dfs = parse_tables_safely(html)
    best = choose_best_table(dfs) if dfs else None
    if best is not None and not best.empty:
        best = clean_table(best, task, modal_url, as_of)
        best.to_csv(csv_path, index=False, encoding="utf-8-sig")
        return DownloadResult(True, task.url, task.name, str(csv_path), modal_url, "wisdomtree-table-parse", f"账期: {as_of}")

    if looks_blocked(body_text):
        await save_debug(page, output_dir, task.name, "modal_blocked")
        return DownloadResult(False, task.url, task.name, note="holdings 页面被拦截")

    await save_debug(page, output_dir, task.name, "modal_no_table")
    return DownloadResult(False, task.url, task.name, note="modal 页面未识别到持仓表")

async def fetch_one_once(task: Job, output_dir: Path, overwrite: bool, show: bool) -> DownloadResult:
    csv_path = output_dir / f"{safe_name(task.name)}.csv"
    if csv_path.exists() and not overwrite:
        return DownloadResult(True, task.url, task.name, str(csv_path), via="pre-cache", note="已存在，跳过")

    async with async_playwright() as p:
        # 【升级2：注入隐形参数与真实视窗环境】
        browser = await p.chromium.launch(
            headless=not show,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--no-sandbox',
                f'--window-size={random.randint(1366, 1920)},{random.randint(768, 1080)}'
            ]
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={'width': 1920, 'height': 1080},
            locale='en-US',
            timezone_id='America/New_York'
        )
        
        # 【升级3：强力反爬指纹擦除 (Stealth JS 注入)】
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            window.chrome = { runtime: {} };
        """)
        
        page = await context.new_page()
        try:
            print(f"[START] {task.name} -> {task.url}", flush=True)
            # 【升级4：增加随机延迟，模拟人类操作】
            await page.goto(task.url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(random.uniform(2.5, 5.0))
            
            try: await page.wait_for_load_state("networkidle", timeout=12000)
            except: pass
            
            # 【核心突破点】：页面加载完后稍微滚动一下，骗过行为检测
            await page.mouse.wheel(0, random.randint(300, 800))
            await asyncio.sleep(random.uniform(1.0, 3.0))

            modal_url = await find_all_holdings_modal_url(page)
            if not modal_url:
                clicked = await open_view_all_holdings_inline(page)
                if clicked:
                    try: await page.wait_for_load_state("networkidle", timeout=10000)
                    except: pass
                    await asyncio.sleep(random.uniform(2.0, 4.0))
                    result = await try_parse_and_save(page, task, output_dir, csv_path, page.url)
                    if result: return result
                await save_debug(page, output_dir, task.name, "fund_page_no_modal")
                return DownloadResult(False, task.url, task.name, note="未找到 All Holdings modal 链接")

            modal_page = await context.new_page()
            try:
                # 给弹出层也加上同样的延迟待遇
                await asyncio.sleep(random.uniform(1.5, 3.5))
                await modal_page.goto(modal_url, wait_until="domcontentloaded", timeout=60000)
                try: await modal_page.wait_for_load_state("networkidle", timeout=12000)
                except: pass
                await asyncio.sleep(random.uniform(2.0, 4.5))
                return await try_parse_and_save(modal_page, task, output_dir, csv_path, modal_url)
            finally:
                await modal_page.close()

        except PlaywrightTimeoutError:
            return DownloadResult(False, task.url, task.name, note="页面超时")
        except Exception as e:
            return DownloadResult(False, task.url, task.name, note=f"异常: {e}")
        finally:
            await page.close()
            await context.close()
            await browser.close()

async def fetch_one(task: Job, output_dir: Path, overwrite: bool, show: bool, retries: int) -> DownloadResult:
    total_attempts = retries + 1
    last_result = None
    for attempt in range(1, total_attempts + 1):
        result = await fetch_one_once(task, output_dir, overwrite, show)
        if result.ok: return result
        last_result = result
        if attempt < total_attempts:
            print(f"[RETRY] {task.name} attempt {attempt}/{total_attempts} failed: {result.note}", flush=True)
            # 失败后避避风头，休息长一点
            await asyncio.sleep(random.uniform(5.0, 10.0))
    return last_result or DownloadResult(False, task.url, task.name, note="未知失败")

async def process_single_job(idx: int, total: int, job: Job, output_dir: Path, session: requests.Session, args: argparse.Namespace, sem: asyncio.Semaphore) -> DownloadResult:
    async with sem:
        result = await fetch_one(job, output_dir, getattr(args, "overwrite", False), getattr(args, "show", False), 1)
        if result.ok: print(f"[{idx:03d}/{total}] ✅ 成功 | {job.name} -> {result.saved_path}", flush=True)
        else: print(f"[{idx:03d}/{total}] ❌ 失败 | {job.name} | {result.note}", flush=True)
        return result

def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-o", "--output", default="wisdomtree_holdings")
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--show", action="store_true")
    parser.add_argument("--debug", action="store_true")
    return parser

async def _standalone_async(args):
    input_path = Path(args.input).expanduser().resolve()
    output_dir = Path(args.output).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    jobs = parse_jobs(input_path)
    if not jobs: return 2
    sem = asyncio.Semaphore(max(1, args.concurrency))
    session = build_session()
    results = [await process_single_job(idx, len(jobs), job, output_dir, session, args, sem) for idx, job in enumerate(jobs, 1)]
    ok_count = sum(1 for r in results if r.ok)
    print(f"\n✨ 完成：{ok_count}/{len(results)} 成功。")
    return 0 if ok_count == len(results) else 2

def main(): return asyncio.run(_standalone_async(build_parser().parse_args()))

if __name__ == "__main__": raise SystemExit(main())