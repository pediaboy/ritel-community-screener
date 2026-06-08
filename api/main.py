"""
RITELCOMMUNITY.ID SCREENER — v6.0
FastAPI backend: GoAPI batching, pure-pandas MA20/MACD, Supabase, Telegram, CMS settings
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime
from typing import Optional, List

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client

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

VOLUME_MIN     = 5_000_000
CHANGE_PCT_MIN = 3.0

# ═══════════════════════════════════════════════════════════════════════════════
# APP & SUPABASE
# ═══════════════════════════════════════════════════════════════════════════════
app = FastAPI(title="RITELCOMMUNITY.ID SCREENER v6.0")

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

class SettingsUpdate(BaseModel):
    vip_price:   Optional[float] = None
    bank_account: Optional[str] = None
    wa_channel:  Optional[str] = None
    wa_group:    Optional[str] = None
    ig_link:     Optional[str] = None

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def check_admin(request: Request):
    secret = request.headers.get("X-Admin-Secret") or request.query_params.get("secret", "")
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")

def goapi_request(path: str, retries: int = 3) -> dict:
    url = f"{GOAPI_BASE}{path}"
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=GOAPI_HEADERS, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                raise
    return {}

def send_telegram(message: str):
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
    return [lst[i:i + size] for i in range(0, len(lst), size)]

def compute_indicators(prices: list) -> dict:
    """
    Hitung MA20 dan MACD — pure pandas, zero external TA library.
    """
    result = {"ma20": None, "macd": None, "macd_signal": None, "indicator_triggered": []}
    if len(prices) < 5:
        return result
    s = pd.Series(prices, dtype=float)
    # MA20
    window = 20 if len(s) >= 20 else len(s)
    result["ma20"] = round(float(s.rolling(window=window).mean().iloc[-1]), 2)
    # MACD — butuh minimal 26 data point
    if len(s) >= 26:
        ema12       = s.ewm(span=12, adjust=False).mean()
        ema26       = s.ewm(span=26, adjust=False).mean()
        macd_line   = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        macd_val    = round(float(macd_line.iloc[-1]), 4)
        sig_val     = round(float(signal_line.iloc[-1]), 4)
        result["macd"]        = macd_val
        result["macd_signal"] = sig_val
        if macd_val > sig_val:
            result["indicator_triggered"].append("MACD_BULLISH")
    return result

def ensure_global_settings():
    """Pastikan baris default global_settings ada di Supabase."""
    if not supabase:
        return
    try:
        res = supabase.table("global_settings").select("id").eq("id", 1).execute()
        if not res.data:
            supabase.table("global_settings").insert({
                "id": 1,
                "vip_price": 99000,
                "bank_account": "BCA 1234567890 a/n RITEL COMMUNITY",
                "wa_channel": "https://whatsapp.com/channel/ritelcommunity",
                "wa_group": "https://chat.whatsapp.com/ritelcommunity",
                "ig_link": "https://instagram.com/ritelcommunity"
            }).execute()
            print("[INIT] global_settings default row created")
    except Exception as e:
        print(f"[WARN] ensure_global_settings: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# SYMBOL LIST — FALLBACK STATIS TOP 100 IDX
# ═══════════════════════════════════════════════════════════════════════════════
TOP100_IDX = [
    "BBCA","BBRI","BMRI","TLKM","ASII","UNVR","ICBP","INDF","KLBF","GGRM",
    "HMSP","PTBA","ADRO","BYAN","ANTM","INCO","VALE","MDKA","SMGR","INKP",
    "TPIA","BRPT","PGEO","MEDC","EMTK","MIKA","SILO","HEAL","MYOR","CPIN",
    "JPFA","MAIN","TBLA","AALI","LSIP","SIMP","PALM","DSNG","SSMS","BWPT",
    "BSDE","CTRA","LPKR","PWON","SMRA","DMAS","JRPT","KIJA","MKPI","PLIN",
    "BBNI","BBTN","BDMN","MEGA","BNII","BNGA","NISP","PNBN","BJTM","BJBR",
    "GOTO","BUKA","TBIG","TOWR","EXCL","ISAT","FREN","MTEL","LINK","AADI",
    "JSMR","WIKA","WSKT","PTPP","ADHI","NRCA","TOTL","ACST","SSIA","CMNP",
    "INTP","SMCB","WTON","ARNA","TOTO","MLIA","AMFG","KIAS","MARK","SRSN",
    "PGAS","ELSA","AKRA","RALS","MAPI","ACES","LPPF","MPPA","AMRT","MIDI",
]

# ═══════════════════════════════════════════════════════════════════════════════
# CORE: GoAPI FETCH & SCREENER
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_lq45_symbols() -> List[str]:
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

def fetch_db_symbols() -> List[str]:
    if not supabase:
        return []
    try:
        result = supabase.table("companies").select("symbol").execute()
        return [r["symbol"] for r in result.data if r.get("symbol")]
    except Exception:
        return []

async def run_goapi_fetch() -> dict:
    start_time = time.time()
    log_api("fetchGoAPI", "INFO", "Mulai fetch harga saham IDX...")

    symbols = fetch_lq45_symbols()
    if not symbols:
        symbols = TOP100_IDX
    db_syms = fetch_db_symbols()
    all_symbols = list(dict.fromkeys(symbols + db_syms))[:200]

    print(f"[GoAPI] Total symbols: {len(all_symbols)}")

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
            time.sleep(0.5)
        except Exception as e:
            errors += 1
            print(f"[GoAPI chunk {i+1}] Error: {e}")

    if not all_results:
        log_api("fetchGoAPI", "ERROR", "Tidak ada data dari server data bursa")
        return {"status": "error", "message": "Tidak ada data", "count": 0}

    upserted = 0
    screener_hits = []
    upsert_batch = []

    for item in all_results:
        ticker     = item.get("symbol", "")
        close      = item.get("close")
        volume     = item.get("volume")
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

        indicators_hit = []
        if volume and int(volume) >= VOLUME_MIN:
            indicators_hit.append(f"VOL>{VOLUME_MIN // 1_000_000}M")
        if change_pct and float(change_pct) >= CHANGE_PCT_MIN:
            indicators_hit.append(f"NAIK+{round(float(change_pct), 2)}%")

        if indicators_hit:
            screener_hits.append({
                "ticker":    ticker,
                "price":     float(close),
                "volume":    int(volume) if volume else 0,
                "change_pct": round(float(change_pct), 4) if change_pct else 0.0,
                "indicators": indicators_hit
            })

    if supabase and upsert_batch:
        for batch in chunk_list(upsert_batch, 50):
            try:
                supabase.table("stocks_data").upsert(
                    batch, on_conflict="ticker"
                ).execute()
                upserted += len(batch)
            except Exception as e:
                print(f"[SUPABASE upsert error] {e}")

    for hit in screener_hits[:10]:
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
        send_telegram(
            f"*SCREENER ALERT*\n"
            f"Ticker: `{hit['ticker']}`\n"
            f"Harga: Rp {hit['price']:,.0f}\n"
            f"Volume: {hit['volume']:,}\n"
            f"Change: {hit['change_pct']:+.2f}%\n"
            f"Trigger: {indicator_str}"
        )

    elapsed = round(time.time() - start_time, 2)
    summary = f"Selesai: {upserted} upserted, {len(screener_hits)} hits, {elapsed}s, {errors} errors"
    log_api("fetchGoAPI", "SUCCESS" if errors == 0 else "WARNING", summary)

    return {
        "status":          "success",
        "symbols_fetched": len(all_results),
        "upserted":        upserted,
        "screener_hits":   len(screener_hits),
        "chunk_errors":    errors,
        "elapsed_sec":     elapsed,
        "hits":            screener_hits[:10]
    }

# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════════════════════
@app.on_event("startup")
async def startup_event():
    ensure_global_settings()

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
# ROUTES — SETTINGS (CMS)
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/settings")
async def get_settings():
    if not supabase:
        return {"id": 1, "vip_price": 99000, "bank_account": "-", "wa_channel": "#", "wa_group": "#", "ig_link": "#"}
    try:
        res = supabase.table("global_settings").select("*").eq("id", 1).execute()
        if res.data:
            return res.data[0]
        return {"id": 1, "vip_price": 99000, "bank_account": "-", "wa_channel": "#", "wa_group": "#", "ig_link": "#"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/admin/settings")
async def update_settings(payload: SettingsUpdate, request: Request):
    check_admin(request)
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase not connected")
    data = {k: v for k, v in payload.dict().items() if v is not None}
    if not data:
        raise HTTPException(status_code=400, detail="Tidak ada data untuk diupdate")
    try:
        res = supabase.table("global_settings").select("id").eq("id", 1).execute()
        if res.data:
            supabase.table("global_settings").update(data).eq("id", 1).execute()
        else:
            data["id"] = 1
            supabase.table("global_settings").insert(data).execute()
        return {"status": "success", "message": "Pengaturan berhasil disimpan", "updated": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — GOAPI FETCH
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/fetch-goapi")
async def fetch_goapi_bg(background_tasks: BackgroundTasks, request: Request):
    check_admin(request)
    background_tasks.add_task(run_goapi_fetch)
    return {"status": "started", "message": "Fetch berjalan di background"}

@app.get("/api/fetch-goapi-sync")
async def fetch_goapi_sync(request: Request):
    check_admin(request)
    result = await run_goapi_fetch()
    return result

@app.post("/api/cron-fetch-goapi")
async def cron_fetch(request: Request, background_tasks: BackgroundTasks):
    secret = request.headers.get("X-Admin-Secret", "")
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403)
    background_tasks.add_task(run_goapi_fetch)
    return {"status": "started"}

@app.post("/api/cron-scrape")
async def cron_scrape_compat(request: Request, background_tasks: BackgroundTasks):
    secret = request.headers.get("X-Admin-Secret", "")
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403)
    background_tasks.add_task(run_goapi_fetch)
    return {"status": "started", "note": "v6.0 — using live data feed"}

@app.post("/api/cron-sync-companies")
async def cron_sync_compat(request: Request):
    secret = request.headers.get("X-Admin-Secret", "")
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403)
    return {"status": "ok"}

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — STOCKS
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/stocks")
async def get_stocks(limit: int = 100, sort: str = "volume"):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase not connected")
    try:
        col = sort if sort in ["price", "volume", "change_pct", "ticker"] else "volume"
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
# ROUTES — ALERTS
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/alerts")
async def get_alerts(limit: int = 50):
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
        existing = supabase.table("users").select("*").eq("phone_number", phone).execute()
        if existing.data:
            supabase.table("users").update({"status": "VIP"}).eq("phone_number", phone).execute()
            send_telegram(f"*USER UPGRADE VIP*\nPhone: `{phone}`\nNama: {existing.data[0].get('name', '-')}\nFree => VIP")
            return {"status": "success", "message": f"User {phone} berhasil diupgrade ke VIP"}
        else:
            data = {"phone_number": phone, "status": "VIP"}
            if payload.name:
                data["name"] = payload.name
            supabase.table("users").insert(data).execute()
            send_telegram(f"*USER BARU VIP*\nPhone: `{phone}`\nNama: {payload.name or '-'}")
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
# ROUTES — ADMIN STATS & LOGS
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/admin/stats")
async def admin_stats(request: Request):
    check_admin(request)
    if not supabase:
        return {"supabase": False}
    try:
        stocks = supabase.table("stocks_data").select("ticker", count="exact").execute()
        alerts = supabase.table("screener_alerts").select("id", count="exact").execute()
        users  = supabase.table("users").select("id", count="exact").execute()
        vip    = supabase.table("users").select("id", count="exact").eq("status", "VIP").execute()
        feed_ok = False
        try:
            r = requests.get(f"{GOAPI_BASE}/stock/idx/index/LQ45/items", headers=GOAPI_HEADERS, timeout=5)
            feed_ok = r.status_code == 200
        except Exception:
            pass
        return {
            "stocks_tracked": stocks.count,
            "total_alerts":   alerts.count,
            "total_users":    users.count,
            "vip_users":      vip.count,
            "feed_status":    "ONLINE" if feed_ok else "OFFLINE",
            "supabase":       True
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/admin/feed-status")
async def feed_status(request: Request):
    check_admin(request)
    try:
        r = requests.get(f"{GOAPI_BASE}/stock/idx/index/LQ45/items", headers=GOAPI_HEADERS, timeout=8)
        if r.status_code == 200:
            d = r.json()
            cnt = len(d.get("data", {}).get("results", []) or d.get("data", []))
            return {"status": "ONLINE", "items": cnt}
        return {"status": "OFFLINE", "http_code": r.status_code}
    except Exception as e:
        return {"status": "OFFLINE", "error": str(e)}

@app.get("/api/admin/logs")
async def admin_logs(request: Request, limit: int = 50):
    check_admin(request)
    if not supabase:
        raise HTTPException(status_code=503)
    try:
        result = supabase.table("api_logs").select("*").order("created_at", desc=True).limit(limit).execute()
        return {"status": "success", "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "version": "6.0",
        "supabase": supabase is not None,
        "feed_key_set": bool(GOAPI_KEY),
        "telegram_set": bool(TELEGRAM_TOKEN and TELEGRAM_CHAT)
    }

# ═══════════════════════════════════════════════════════════════════════════════
# ROUTES — COMPANIES (compat)
# ═══════════════════════════════════════════════════════════════════════════════
@app.get("/api/companies")
async def get_companies(q: str = "", limit: int = 50, offset: int = 0):
    if not supabase:
        raise HTTPException(status_code=503)
    try:
        query = supabase.table("companies").select("symbol,name,logo,sector")
        if q:
            query = query.or_(f"symbol.ilike.%{q}%,name.ilike.%{q}%")
        result = query.order("symbol").range(offset, offset + limit - 1).execute()
        return {"status": "success", "count": len(result.data), "data": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
