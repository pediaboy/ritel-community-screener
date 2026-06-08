"""
Ritel Community Screener — v5.0 (GoAPI Full Integration)
FastAPI backend: GoAPI batching, pandas-ta indicators, Supabase upsert, Telegram alerts
"""

import os
import math
import time
import asyncio
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client

# ── Pandas-TA (graceful fallback) ─────────────────────────────────────────────
try:
    import pandas_ta as ta
    HAS_TA = True
except Exception:
    try:
        import ta as ta_lib
        HAS_TA = False  # use ta_lib fallback
    except Exception:
        HAS_TA = False
        ta_lib = None

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
SUPABASE_URL   = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY   = os.getenv("SUPABASE_KEY", "")
ADMIN_SECRET   = os.getenv("ADMIN_SECRET", "pedia123")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")
GOAPI_KEY      = os.getenv("GOAPI_KEY", "")
GOAPI_BASE     = "https://api.goapi.io"
GOAPI_HEADERS  = {"Authorization": GOAPI_KEY, "accept": "application/json"}

# Screener thresholds
VOLUME_MIN     = 5_000_000
CHANGE_PCT_MIN = 3.0   # % kenaikan minimum untuk alert

# ═══════════════════════════════════════════════════════════════════════════════
# APP & SUPABASE INIT
# ═══════════════════════════════════════════════════════════════════════════════
app = FastAPI(title="Ritel Community Screener v5.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"[WARN] Supabase init failed: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# MODELS
# ═══════════════════════════════════════════════════════════════════════════════
class UpgradeUserRequest(BaseModel):
    phone_number: str
    name: Optional[str] = None

class AdminAuth(BaseModel):
    secret: str

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def check_admin(request: Request):
    secret = request.headers.get("X-Admin-Secret") or request.query_params.get("secret", "")
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")

def goapi_request(path: str, params: dict = None, retries: int = 3) -> dict:
    url = f"{GOAPI_BASE}{path}"
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=GOAPI_HEADERS, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                raise
    return {}

def send_telegram(message: str):
    """Send Telegram notification."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": message, "parse_mode": "Markdown"},
            timeout=10
        )
    except Exception as e:
        print(f"[TELEGRAM ERROR] {e}")

def log_api(service: str, status: str, message: str):
    """Log ke Supabase api_logs."""
    if not supabase:
        return
    try:
        supabase.table("api_logs").insert({
            "service_name": service,
            "status": status,
            "message": message
        }).execute()
    except Exception as e:
        print(f"[LOG ERROR] {e}")

def chunk_list(lst: list, size: int) -> list:
    return [lst[i:i+size] for i in range(0, len(lst), size)]

def compute_indicators(prices: list) -> dict:
    """
    Hitung MA20 dan MACD dari list harga close.
    Minimal 26 data points untuk MACD.
    Returns: {ma20, macd, macd_signal, indicator_triggered}
    """
    result = {"ma20": None, "macd": None, "macd_signal": None, "indicator_triggered": []}
    if len(prices) < 5:
        return result
    s = pd.Series(prices, dtype=float)
    # MA20
    if len(s) >= 20:
        result["ma20"] = round(float(s.rolling(20).mean().iloc[-1]), 2)
    elif len(s) >= 5:
        result["ma20"] = round(float(s.rolling(len(s)).mean().iloc[-1]), 2)
    # MACD
    if len(s) >= 26:
        if HAS_TA:
            try:
                macd_df = ta.macd(s, fast=12, slow=26, signal=9)
                if macd_df is not None and not macd_df.empty:
                    cols = macd_df.columns.tolist()
                    macd_val = float(macd_df[cols[0]].iloc[-1])
                    sig_val  = float(macd_df[cols[2]].iloc[-1])
                    result["macd"] = round(macd_val, 4)
                    result["macd_signal"] = round(sig_val, 4)
                    if macd_val > sig_val:
                        result["indicator_triggered"].append("MACD_BULLISH")
            except Exception:
                pass
    return result

# ═══════════════════════════════════════════════════════════════════════════════
# CORE: GoAPI FETCH & SCREENER
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_lq45_symbols() -> List[str]:
    """Fetch daftar LQ45 dari GoAPI."""
    try:
        d = goapi_request("/stock/idx/index/LQ45/items")
        items = d.get("data", {}).get("results", []) or d.get("data", [])
        if items and isinstance(items[0], str):
            return items
        elif items and isinstance(items[0], dict):
            return [x.get("symbol", "") for x in items if x.get("symbol")]
    except Exception as e:
        print(f"[LQ45 ERROR] {e}")
    return []

def fetch_idxall_symbols() -> List[str]:
    """Fetch semua symbol IDX dari companies table (Supabase)."""
    if not supabase:
        return []
    try:
        result = supabase.table("companies").select("symbol").execute()
        return [r["symbol"] for r in result.data if r.get("symbol")]
    except Exception:
        return []

# Top 100 IDX saham paling aktif — fallback statis
TOP100_IDX = [
    "BBCA","BBRI","BMRI","TLKM","ASII","UNVR","ICBP","INDF","KLBF","GGRM",
    "HMSP","PTBA","ADRO","BYAN","ANTM","INCO","VALE","MDKA","SMGR","INKP",
    "TPIA","BRPT","PGEO","MEDC","EMTK","MIKA","SILO","HEAL","MYOR","CPIN",
    "JPFA","MAIN","TBLA","AALI","LSIP","SIMP","PALM","DSNG","SSMS","BWPT",
    "BSDE","CTRA","LPKR","PWON","SMRA","DMAS","JRPT","KIJA","MKPI","PLIN",
    "BBNI","BBTN","BDMN","MEGA","BNII","BNGA","NISP","PNBN","BJTM","BJBR",
    "GOTO","BUKA","EMTK","TBIG","TOWR","EXCL","ISAT","FREN","MTEL","LINK",
    "JSMR","WIKA","WSKT","PTPP","ADHI","NRCA","TOTL","ACST","SSIA","CMNP",
    "INTP","SMCB","WTON","ARNA","TOTO","MLIA","AMFG","KIAS","MARK","SRSN",
    "PGAS","ELSA","AKRA","RALS","MAPI","ACES","LPPF","MPPA","AMRT","MIDI",
]

async def run_goapi_fetch() -> dict:
    """
    Core function: fetch harga dari GoAPI, batch max 50,
    compute indicators, upsert Supabase, kirim Telegram jika ada screener hit.
    """
    start_time = time.time()
    log_api("fetchGoAPI", "INFO", "Mulai fetch GoAPI prices...")

    # 1. Ambil symbol list
    symbols = fetch_lq45_symbols()
    if not symbols:
        symbols = TOP100_IDX
    # Tambah IDX companies dari Supabase (max 200 total)
    db_symbols = fetch_idxall_symbols()
    all_symbols = list(dict.fromkeys(symbols + db_symbols))[:200]

    print(f"[GoAPI] Total symbols: {len(all_symbols)}")

    # 2. Pecah jadi chunks max 50
    chunks = chunk_list(all_symbols, 50)
    all_results = []
    errors = 0

    for i, chunk in enumerate(chunks):
        sym_str = ",".join(chunk)
        try:
            d = goapi_request(f"/stock/idx/prices?symbols={sym_str}")
            results = d.get("data", {}).get("results", []) or d.get("data", [])
            if isinstance(results, list):
                all_results.extend(results)
            time.sleep(0.5)  # rate limit courtesy
        except Exception as e:
            errors += 1
            print(f"[GoAPI chunk {i+1}] Error: {e}")

    print(f"[GoAPI] Fetched {len(all_results)} price records, {errors} chunk errors")

    if not all_results:
        log_api("fetchGoAPI", "ERROR", "Tidak ada data dari GoAPI")
        return {"status": "error", "message": "Tidak ada data dari GoAPI", "count": 0}

    # 3. Upsert ke Supabase & cek screener
    upserted = 0
    screener_hits = []
    upsert_batch = []

    for item in all_results:
        ticker = item.get("symbol", "")
        close  = item.get("close")
        volume = item.get("volume")
        change_pct = item.get("change_pct")
        if not ticker or close is None:
            continue

        row = {
            "ticker":     ticker,
            "price":      float(close),
            "volume":     int(volume) if volume else 0,
            "change_pct": round(float(change_pct), 4) if change_pct else 0.0,
            "updated_at": datetime.utcnow().isoformat()
        }
        upsert_batch.append(row)

        # Screener check: volume besar + kenaikan signifikan
        indicators_hit = []
        if volume and int(volume) >= VOLUME_MIN:
            indicators_hit.append(f"VOL>{VOLUME_MIN//1_000_000}M")
        if change_pct and float(change_pct) >= CHANGE_PCT_MIN:
            indicators_hit.append(f"NAIK+{round(float(change_pct),2)}%")

        if indicators_hit:
            screener_hits.append({
                "ticker":   ticker,
                "price":    float(close),
                "volume":   int(volume) if volume else 0,
                "change_pct": round(float(change_pct), 4) if change_pct else 0.0,
                "indicators": indicators_hit
            })

    # Batch upsert ke stocks_data
    if supabase and upsert_batch:
        for batch in chunk_list(upsert_batch, 50):
            try:
                supabase.table("stocks_data").upsert(
                    batch, on_conflict="ticker"
                ).execute()
                upserted += len(batch)
            except Exception as e:
                print(f"[SUPABASE upsert error] {e}")

    # 4. Insert screener_alerts & kirim Telegram
    for hit in screener_hits[:10]:  # max 10 alert per run
        indicator_str = " | ".join(hit["indicators"])
        if supabase:
            try:
                supabase.table("screener_alerts").insert({
                    "ticker":              hit["ticker"],
                    "price":              hit["price"],
                    "indicator_triggered": indicator_str,
                    "timestamp":           datetime.utcnow().isoformat()
                }).execute()
            except Exception:
                pass
        # Telegram notification
        msg = (
            f"🚨 *SCREENER ALERT*\n"
            f"Ticker: `{hit['ticker']}`\n"
            f"Harga: Rp {hit['price']:,.0f}\n"
            f"Volume: {hit['volume']:,}\n"
            f"Change: {hit['change_pct']:+.2f}%\n"
            f"Trigger: {indicator_str}"
        )
        send_telegram(msg)

    elapsed = round(time.time() - start_time, 2)
    summary = (
        f"fetchGoAPI selesai: {upserted} upserted, "
        f"{len(screener_hits)} screener hits, {elapsed}s, {errors} errors"
    )
    log_api("fetchGoAPI", "SUCCESS" if errors == 0 else "WARNING", summary)

    return {
        "status":         "success",
        "symbols_fetched": len(all_results),
        "upserted":        upserted,
        "screener_hits":   len(screener_hits),
        "chunk_errors":    errors,
        "elapsed_sec":     elapsed,
        "hits":            screener_hits[:10]
    }

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — PAGES
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("static/index.html") as f:
        return f.read()

@app.get("/admin", response_class=HTMLResponse)
async def admin_page():
    with open("static/admin.html") as f:
        return f.read()

@app.get("/pricing", response_class=HTMLResponse)
async def pricing_page():
    with open("static/pricing.html") as f:
        return f.read()

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — GOAPI
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/fetch-goapi")
async def fetch_goapi_endpoint(background_tasks: BackgroundTasks, request: Request):
    """Trigger GoAPI fetch. Admin only. Runs as background task."""
    check_admin(request)
    background_tasks.add_task(run_goapi_fetch)
    return {"status": "started", "message": "GoAPI fetch berjalan di background (~30-60 detik)"}

@app.get("/api/fetch-goapi-sync")
async def fetch_goapi_sync(request: Request):
    """Sync version — tunggu sampai selesai, return hasilnya langsung."""
    check_admin(request)
    result = await run_goapi_fetch()
    return result

@app.post("/api/cron-fetch-goapi")
async def cron_fetch_goapi(request: Request, background_tasks: BackgroundTasks):
    """Cron trigger untuk GoAPI fetch."""
    secret = request.headers.get("X-Admin-Secret", "")
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403)
    background_tasks.add_task(run_goapi_fetch)
    return {"status": "started"}

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — STOCKS DATA
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/stocks")
async def get_stocks(limit: int = 100, sort: str = "volume"):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase not connected")
    try:
        col = sort if sort in ["price","volume","change_pct","ticker"] else "volume"
        result = supabase.table("stocks_data").select("*").order(col, desc=True).limit(limit).execute()
        return {"status": "success", "count": len(result.data), "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stocks/{ticker}")
async def get_stock(ticker: str):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase not connected")
    try:
        result = supabase.table("stocks_data").select("*").eq("ticker", ticker.upper()).execute()
        if not result.data:
            raise HTTPException(status_code=404, detail=f"Ticker {ticker} tidak ditemukan")
        return {"status": "success", "data": result.data[0]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — SCREENER ALERTS
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/alerts")
async def get_alerts(limit: int = 50, request: Request = None):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase not connected")
    try:
        result = supabase.table("screener_alerts").select("*").order("timestamp", desc=True).limit(limit).execute()
        return {"status": "success", "count": len(result.data), "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — USERS
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/users")
async def get_users(request: Request):
    check_admin(request)
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase not connected")
    try:
        result = supabase.table("users").select("*").order("created_at", desc=True).execute()
        return {"status": "success", "count": len(result.data), "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/admin/upgrade-user")
async def upgrade_user(payload: UpgradeUserRequest, request: Request):
    check_admin(request)
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase not connected")
    phone = payload.phone_number.strip()
    try:
        # Cek user exists
        existing = supabase.table("users").select("*").eq("phone_number", phone).execute()
        if existing.data:
            # Update ke VIP
            supabase.table("users").update({"status": "VIP"}).eq("phone_number", phone).execute()
            # Telegram notif
            send_telegram(
                f"✅ *USER UPGRADE VIP*\n"
                f"Phone: `{phone}`\n"
                f"Nama: {existing.data[0].get('name', '-')}\n"
                f"Status: Free → *VIP*"
            )
            return {"status": "success", "message": f"User {phone} berhasil diupgrade ke VIP"}
        else:
            # Insert baru sebagai VIP
            data = {"phone_number": phone, "status": "VIP"}
            if payload.name:
                data["name"] = payload.name
            supabase.table("users").insert(data).execute()
            send_telegram(
                f"✅ *USER BARU VIP*\n"
                f"Phone: `{phone}`\n"
                f"Nama: {payload.name or '-'}"
            )
            return {"status": "success", "message": f"User {phone} dibuat dan langsung VIP"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/users/register")
async def register_user(payload: UpgradeUserRequest):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase not connected")
    phone = payload.phone_number.strip()
    try:
        existing = supabase.table("users").select("id").eq("phone_number", phone).execute()
        if existing.data:
            return {"status": "exists", "message": "Nomor sudah terdaftar"}
        data = {"phone_number": phone, "status": "Free"}
        if payload.name:
            data["name"] = payload.name
        supabase.table("users").insert(data).execute()
        return {"status": "success", "message": "Pendaftaran berhasil"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — COMPANIES
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/companies")
async def get_companies(q: str = "", limit: int = 50, offset: int = 0):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase not connected")
    try:
        query = supabase.table("companies").select("symbol,name,logo,sector")
        if q:
            query = query.or_(f"symbol.ilike.%{q}%,name.ilike.%{q}%")
        result = query.order("symbol").range(offset, offset + limit - 1).execute()
        return {"status": "success", "count": len(result.data), "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — ADMIN MISC
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/admin/logs")
async def admin_logs(request: Request, limit: int = 50):
    check_admin(request)
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase not connected")
    try:
        result = supabase.table("api_logs").select("*").order("created_at", desc=True).limit(limit).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/admin/stats")
async def admin_stats(request: Request):
    check_admin(request)
    if not supabase:
        return {"supabase": False}
    try:
        stocks = supabase.table("stocks_data").select("ticker", count="exact").execute()
        alerts = supabase.table("screener_alerts").select("id", count="exact").execute()
        users  = supabase.table("users").select("id", count="exact").execute()
        vip    = supabase.table("users").select("id", count="exact").eq("status","VIP").execute()
        # GoAPI health check
        goapi_ok = False
        try:
            r = requests.get(f"{GOAPI_BASE}/stock/idx/index/LQ45/items",
                           headers=GOAPI_HEADERS, timeout=5)
            goapi_ok = r.status_code == 200
        except Exception:
            pass
        return {
            "stocks_tracked": stocks.count,
            "total_alerts":   alerts.count,
            "total_users":    users.count,
            "vip_users":      vip.count,
            "goapi_status":   "ONLINE" if goapi_ok else "OFFLINE",
            "supabase":       True
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/admin/goapi-status")
async def goapi_status_check(request: Request):
    check_admin(request)
    try:
        r = requests.get(f"{GOAPI_BASE}/stock/idx/index/LQ45/items",
                        headers=GOAPI_HEADERS, timeout=8)
        if r.status_code == 200:
            d = r.json()
            cnt = len(d.get("data", {}).get("results", []) or d.get("data", []))
            return {"status": "ONLINE", "items": cnt, "key_prefix": GOAPI_KEY[:8] + "..."}
        return {"status": "OFFLINE", "http_code": r.status_code}
    except Exception as e:
        return {"status": "OFFLINE", "error": str(e)}

@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "version": "5.0",
        "supabase": supabase is not None,
        "goapi_key_set": bool(GOAPI_KEY),
        "telegram_set": bool(TELEGRAM_TOKEN and TELEGRAM_CHAT)
    }

# ═══════════════════════════════════════════════════════════════════════════════
# CRON COMPAT (lama)
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/api/cron-scrape")
async def cron_scrape_compat(request: Request, background_tasks: BackgroundTasks):
    """Backward compat — sekarang pakai GoAPI, bukan yfinance."""
    secret = request.headers.get("X-Admin-Secret", "")
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403)
    background_tasks.add_task(run_goapi_fetch)
    return {"status": "started", "note": "Redirected to GoAPI fetch (v5.0)"}

@app.post("/api/cron-sync-companies")
async def cron_sync_compat(request: Request):
    """Backward compat endpoint."""
    secret = request.headers.get("X-Admin-Secret", "")
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403)
    return {"status": "ok", "note": "Companies sync via /api/companies, harga via /api/fetch-goapi"}
