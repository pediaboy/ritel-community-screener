"""
Ritel Community Screener — Backend v4.0
Integrasi: GoAPI IDX + yfinance + Supabase
"""
import os
import time
import random
import requests
import pandas as pd
import yfinance as yf
import pathlib
from datetime import datetime, timezone
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Depends, Header, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

# ─────────────────────────────────────────
# ENV
# ─────────────────────────────────────────
SUPABASE_URL       = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY       = os.getenv("SUPABASE_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
ADMIN_SECRET       = os.getenv("ADMIN_SECRET", "pedia123")
GOAPI_KEY          = os.getenv("GOAPI_KEY", "")
GOAPI_BASE_URL     = "https://api.goapi.io"

# ─────────────────────────────────────────
# SUPABASE CLIENT
# ─────────────────────────────────────────
_sb = None
def get_supabase():
    global _sb
    if _sb is None and SUPABASE_URL and SUPABASE_KEY:
        try:
            from supabase import create_client
            _sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        except Exception as e:
            print(f"[Supabase] Init error: {e}")
    return _sb

# ─────────────────────────────────────────
# API LOG — catat setiap request ke GoAPI
# ─────────────────────────────────────────
def write_api_log(service_name: str, status: str, message: str):
    """Catat hasil request ke tabel api_logs."""
    try:
        sb = get_supabase()
        if not sb:
            return
        sb.table("api_logs").insert({
            "service_name": service_name,
            "status":       status,
            "message":      message[:500],
            "created_at":   datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception as e:
        print(f"[api_logs] Write error: {e}")

# ─────────────────────────────────────────
# GOAPI SERVICE
# ─────────────────────────────────────────
GOAPI_TIMEOUT = 15
GOAPI_MAX_RETRY = 3

def goapi_request(endpoint: str, params: dict = None) -> dict:
    """
    HTTP GET ke GoAPI dengan:
    - Retry otomatis 3x
    - Timeout 15 detik
    - API key dari ENV (tidak hardcode)
    - Logging ke api_logs
    """
    if not GOAPI_KEY:
        write_api_log("goapi", "ERROR", "GOAPI_KEY tidak ditemukan di environment")
        raise ValueError("GOAPI_KEY tidak diset di environment variable")

    url = f"{GOAPI_BASE_URL}/{endpoint.lstrip('/')}"
    query = {"api_key": GOAPI_KEY}
    if params:
        query.update(params)

    last_error = None
    for attempt in range(1, GOAPI_MAX_RETRY + 1):
        try:
            resp = requests.get(url, params=query, timeout=GOAPI_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") == "success":
                write_api_log("goapi", "SUCCESS", f"{endpoint} → {data.get('message','OK')}")
                return data
            else:
                msg = data.get("message", "Unknown error")
                write_api_log("goapi", "ERROR", f"{endpoint} → {msg}")
                raise ValueError(f"GoAPI error: {msg}")

        except requests.exceptions.Timeout:
            last_error = f"Timeout (attempt {attempt}/{GOAPI_MAX_RETRY})"
            print(f"[GoAPI] {last_error}")
        except requests.exceptions.RequestException as e:
            last_error = f"Request error: {e} (attempt {attempt}/{GOAPI_MAX_RETRY})"
            print(f"[GoAPI] {last_error}")
        except ValueError as e:
            write_api_log("goapi", "ERROR", str(e))
            raise

        if attempt < GOAPI_MAX_RETRY:
            time.sleep(2 ** attempt)   # exponential backoff: 2s, 4s

    write_api_log("goapi", "ERROR", f"{endpoint} gagal setelah {GOAPI_MAX_RETRY}x retry: {last_error}")
    raise RuntimeError(f"GoAPI {endpoint} gagal setelah {GOAPI_MAX_RETRY}x retry: {last_error}")


def goapi_status() -> dict:
    """Cek apakah GoAPI aktif. Return {online, message}."""
    try:
        goapi_request("stock/idx/companies")
        return {"online": True, "status": "ONLINE"}
    except Exception as e:
        return {"online": False, "status": "OFFLINE", "error": str(e)}


# ─────────────────────────────────────────
# syncCompanies — ambil data GoAPI → Supabase
# ─────────────────────────────────────────
def syncCompanies() -> dict:
    """
    Fetch 973 companies dari GoAPI IDX,
    validasi response, upsert ke Supabase berdasarkan symbol (no duplicate).
    """
    print("[syncCompanies] Memulai sync dari GoAPI...")
    start = time.time()

    # 1. Fetch dari GoAPI
    try:
        resp = goapi_request("stock/idx/companies")
    except Exception as e:
        return {"success": False, "error": str(e), "saved": 0}

    # 2. Validasi response
    results = resp.get("data", {}).get("results", [])
    if not results:
        write_api_log("syncCompanies", "ERROR", "Response kosong atau format tidak dikenali")
        return {"success": False, "error": "Data kosong dari GoAPI", "saved": 0}

    print(f"[syncCompanies] Dapat {len(results)} companies dari GoAPI")

    # 3. Upsert ke Supabase
    sb = get_supabase()
    if not sb:
        return {"success": False, "error": "Supabase tidak tersambung", "saved": 0}

    now_ts = datetime.now(timezone.utc).isoformat()
    saved  = 0
    errors = 0

    # Batch upsert per 50 untuk efisiensi
    batch_size = 50
    for i in range(0, len(results), batch_size):
        batch = results[i : i + batch_size]
        rows  = []
        for item in batch:
            sym = (item.get("symbol") or "").strip().upper()
            if not sym:
                continue
            rows.append({
                "symbol":     sym,
                "name":       (item.get("name") or "").strip(),
                "logo":       item.get("logo", ""),
                "updated_at": now_ts,
            })

        if not rows:
            continue

        try:
            sb.table("companies").upsert(rows, on_conflict="symbol").execute()
            saved += len(rows)
        except Exception as e:
            errors += len(rows)
            print(f"[syncCompanies] Batch {i//batch_size+1} error: {e}")

    elapsed = round(time.time() - start, 2)
    msg = f"Sync selesai: {saved} companies disimpan, {errors} error, {elapsed}s"
    write_api_log("syncCompanies", "SUCCESS" if saved > 0 else "ERROR", msg)
    print(f"[syncCompanies] {msg}")

    return {
        "success": True,
        "total_from_api": len(results),
        "saved":          saved,
        "errors":         errors,
        "elapsed_sec":    elapsed,
        "timestamp":      now_ts,
    }


# ─────────────────────────────────────────
# STOCK SCRAPER — yfinance batch
# ─────────────────────────────────────────
IDX_TICKERS = [
    "AALI","ACES","ADHI","ADRO","AGII","AGRO","AKRA","AMFG","AMMN","AMRT",
    "ANTM","ARNA","ASII","ASRI","AUTO","BBCA","BBNI","BBRI","BBTN","BDMN",
    "BFIN","BJBR","BJTM","BKSL","BMRI","BMTR","BNGA","BNII","BRMS","BRPT",
    "BSDE","BSSR","BUKA","BULL","BUMI","CAKK","CARR","CASA","CASS","CFIN",
    "CINT","CITA","CMNP","CMRY","CNET","CPIN","CTRA","CUAN","DART","DEWA",
    "DLTA","DMAS","DNET","DOID","DSNG","DUTI","DVLA","EKAD","ELSA","EMDE",
    "EMTK","ERAA","ESSA","FAST","FILM","FREN","GAMA","GDST","GEMS","GGRM",
    "GIAA","GJTL","GOOD","GOTO","GPRA","HEAL","HERO","HMSP","HOKI","HRUM",
    "ICBP","IGAR","IIKP","IMAS","IMPC","INAF","INAI","INCI","INCO","INDF",
    "INDY","INKP","INPP","INTP","IPCC","IPCM","ISAT","ITMG","JAWA","JECC",
    "JKON","JPFA","JRPT","JSMR","KAEF","KBLI","KBLN","KDSI","KIJA","KINO",
    "KLBF","KRAS","LPPF","LSIP","LTLS","MAPI","MBAP","MBSS","MDKA","MEDC",
    "MEGA","MERK","MIKA","MKPI","MLBI","MNCN","MPPA","MREI","MSKY","MTDL",
    "MTEL","MTLA","MTOR","NCKL","NIKL","NISP","NOBU","NRCA","OCAP","PADI",
    "PANR","PANS","PGAS","PJAA","PLIN","PNBN","PNIN","PNLF","PPRE","PPRO",
    "PTBA","PTPP","PTRO","PWON","PYFA","RAJA","RDTX","RIGS","ROTI","SAME",
    "SCCO","SCMA","SGRO","SIDO","SILO","SIMP","SKBM","SKLT","SMBR","SMCB",
    "SMDR","SMGR","SMRA","SMSM","SOHO","SRIL","SRTG","SSIA","SSMS","TBIG",
    "TBLA","TCID","TELE","TINS","TKIM","TLKM","TOBA","TOTL","TOTO","TOWR",
    "TPIA","TRIM","TSPC","TURI","ULTJ","UNIC","UNSP","UNTR","UNVR","VIVA",
    "WEGE","WEHA","WIKA","WINS","WOOD","WSKT","WTON","YPAS","ZINC",
]

def scrape_all_stocks(tickers: List[str] = None) -> dict:
    if tickers is None:
        tickers = IDX_TICKERS
    print(f"[Scraper] Scrape {len(tickers)} tickers via yfinance...")
    start = time.time()
    batch_size = 100
    all_rows   = []

    for i in range(0, len(tickers), batch_size):
        batch   = tickers[i : i + batch_size]
        symbols = [f"{t}.JK" for t in batch]
        try:
            data = yf.download(
                " ".join(symbols), period="2d", interval="1d",
                auto_adjust=True, progress=False, threads=True
            )
            if data.empty:
                continue
            close  = data["Close"].iloc[-1]
            volume = data["Volume"].iloc[-1]
            now_ts = datetime.now(timezone.utc).isoformat()
            for ticker in batch:
                sym   = f"{ticker}.JK"
                price = close.get(sym)
                vol   = volume.get(sym)
                if price is None or str(price) == "nan":
                    continue
                all_rows.append({
                    "ticker":     ticker,
                    "price":      round(float(price), 2),
                    "volume":     float(vol) if vol and str(vol) != "nan" else 0.0,
                    "updated_at": now_ts,
                })
        except Exception as e:
            print(f"[Scraper] Batch {i//batch_size+1} error: {e}")
        time.sleep(random.uniform(1.0, 2.0))

    sb    = get_supabase()
    saved = 0
    if sb and all_rows:
        for row in all_rows:
            try:
                sb.table("stocks_data").upsert(row, on_conflict="ticker").execute()
                saved += 1
            except Exception as e:
                print(f"[DB] {row['ticker']} error: {e}")

    elapsed = round(time.time() - start, 2)
    return {"scraped": len(all_rows), "saved": saved, "elapsed_sec": elapsed,
            "timestamp": datetime.now(timezone.utc).isoformat()}


# ─────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────
def send_telegram(message: str, chat_id: str = None) -> bool:
    target = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or not target:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": target, "text": message, "parse_mode": "HTML"}, timeout=10
        )
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"[Telegram] {e}")
        return False

# ─────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────
app = FastAPI(title="Ritel Community Screener", version="4.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

static_dir = pathlib.Path(__file__).parent.parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

@app.exception_handler(Exception)
async def global_error(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"error": str(exc)})

def require_admin(x_admin_secret: str = Header(...)):
    if x_admin_secret != ADMIN_SECRET:
        raise HTTPException(403, "Admin secret salah.")

# ─── MODELS ───
class UpgradeUserReq(BaseModel):
    phone_number: str

class StockUpsert(BaseModel):
    ticker: str; price: float; volume: float

class AlertCreate(BaseModel):
    ticker: str; price: float; indicator_triggered: str
    telegram_chat_id: Optional[str] = None

# ─────────────────────────────────────────
# ROUTES — FRONTEND
# ─────────────────────────────────────────
@app.get("/")
def index():
    p = static_dir / "index.html"
    return FileResponse(str(p)) if p.exists() else {"message": "Ritel Community Screener v4"}

@app.get("/pricing")
def pricing():
    p = static_dir / "pricing.html"
    return FileResponse(str(p)) if p.exists() else {"error": "not found"}

@app.get("/admin")
def admin_page():
    p = static_dir / "admin.html"
    return FileResponse(str(p)) if p.exists() else {"error": "not found"}

# ─────────────────────────────────────────
# ROUTES — PUBLIC
# ─────────────────────────────────────────
@app.get("/api/health")
def health():
    return {"status": "ok", "version": "4.0.0", "ts": datetime.now(timezone.utc).isoformat()}

@app.get("/api/stocks")
def get_stocks(limit: int = 100, offset: int = 0):
    sb = get_supabase()
    if not sb:
        return {"data": [], "error": "Supabase belum terhubung"}
    try:
        r = (sb.table("stocks_data").select("*")
               .order("volume", desc=True).range(offset, offset + limit - 1).execute())
        return {"data": r.data, "total": len(r.data), "live": True}
    except Exception as e:
        return {"data": [], "error": str(e)}

@app.get("/api/alerts")
def get_alerts(limit: int = 20):
    sb = get_supabase()
    if not sb: return {"data": []}
    try:
        r = (sb.table("screener_alerts").select("*")
               .order("timestamp", desc=True).limit(limit).execute())
        return {"data": r.data}
    except Exception as e:
        return {"data": [], "error": str(e)}

@app.get("/api/tickers")
def get_tickers():
    return {"tickers": IDX_TICKERS, "total": len(IDX_TICKERS)}

# ─── COMPANIES ───
@app.get("/api/companies")
def get_companies(search: str = "", limit: int = 50, offset: int = 0):
    sb = get_supabase()
    if not sb: return {"data": [], "total": 0}
    try:
        q = sb.table("companies").select("*", count="exact")
        if search:
            q = q.or_(f"symbol.ilike.%{search}%,name.ilike.%{search}%")
        r = q.order("symbol").range(offset, offset + limit - 1).execute()
        return {"data": r.data, "total": r.count or 0}
    except Exception as e:
        return {"data": [], "total": 0, "error": str(e)}

@app.get("/api/companies/sync-status")
def companies_sync_status():
    sb = get_supabase()
    if not sb: return {"total": 0, "last_sync": None}
    try:
        count_r = sb.table("companies").select("symbol", count="exact").execute()
        last_r  = (sb.table("companies").select("updated_at")
                     .order("updated_at", desc=True).limit(1).execute())
        log_r   = (sb.table("api_logs").select("*")
                     .eq("service_name", "syncCompanies")
                     .order("created_at", desc=True).limit(1).execute())
        return {
            "total":     count_r.count or 0,
            "last_sync": last_r.data[0]["updated_at"] if last_r.data else None,
            "last_log":  log_r.data[0] if log_r.data else None,
        }
    except Exception as e:
        return {"total": 0, "error": str(e)}

# ─── API LOGS ───
@app.get("/api/admin/logs", dependencies=[Depends(require_admin)])
def get_logs(limit: int = 50, service: str = ""):
    sb = get_supabase()
    if not sb: return {"data": []}
    try:
        q = sb.table("api_logs").select("*").order("created_at", desc=True).limit(limit)
        if service:
            q = q.eq("service_name", service)
        r = q.execute()
        return {"data": r.data}
    except Exception as e:
        return {"data": [], "error": str(e)}

# ─────────────────────────────────────────
# ROUTES — SCRAPING & SYNC
# ─────────────────────────────────────────
@app.post("/api/cron-scrape")
def cron_scrape(background_tasks: BackgroundTasks, x_admin_secret: str = Header(...)):
    if x_admin_secret != ADMIN_SECRET:
        raise HTTPException(403, "Admin secret salah.")
    background_tasks.add_task(scrape_all_stocks, IDX_TICKERS)
    return {"message": f"Scraping {len(IDX_TICKERS)} tickers dimulai!", "ts": datetime.now(timezone.utc).isoformat()}

@app.post("/api/admin/force-scrape", dependencies=[Depends(require_admin)])
def force_scrape(background_tasks: BackgroundTasks):
    background_tasks.add_task(scrape_all_stocks, IDX_TICKERS)
    return {"message": f"Force scrape {len(IDX_TICKERS)} tickers dimulai!", "ts": datetime.now(timezone.utc).isoformat()}

@app.post("/api/admin/sync-companies", dependencies=[Depends(require_admin)])
def sync_companies_endpoint(background_tasks: BackgroundTasks):
    """Trigger syncCompanies manual dari admin panel."""
    background_tasks.add_task(syncCompanies)
    return {"message": "Sync companies dari GoAPI dimulai di background!", "ts": datetime.now(timezone.utc).isoformat()}

@app.post("/api/cron-sync-companies")
def cron_sync_companies(background_tasks: BackgroundTasks, x_admin_secret: str = Header(...)):
    """Endpoint untuk scheduler cron setiap 1 jam (0 * * * *)."""
    if x_admin_secret != ADMIN_SECRET:
        raise HTTPException(403, "Admin secret salah.")
    background_tasks.add_task(syncCompanies)
    return {"message": "Cron sync companies dimulai", "ts": datetime.now(timezone.utc).isoformat()}

@app.get("/api/admin/goapi-status", dependencies=[Depends(require_admin)])
def goapi_status_endpoint():
    """Cek status koneksi GoAPI: ONLINE / OFFLINE."""
    return goapi_status()

@app.get("/api/admin/scrape-status", dependencies=[Depends(require_admin)])
def scrape_status():
    sb = get_supabase()
    if not sb: return {"count": 0}
    latest = (sb.table("stocks_data").select("ticker,updated_at")
                .order("updated_at", desc=True).limit(1).execute())
    total  = sb.table("stocks_data").select("ticker", count="exact").execute()
    return {
        "total_in_db":   total.count if hasattr(total, "count") else 0,
        "latest_update": latest.data[0] if latest.data else None,
    }

# ─────────────────────────────────────────
# ROUTES — ADMIN
# ─────────────────────────────────────────
@app.post("/api/admin/upgrade-user", dependencies=[Depends(require_admin)])
def upgrade_user(req: UpgradeUserReq):
    sb = get_supabase()
    if not sb: raise HTTPException(503, "Supabase tidak tersambung")
    result = sb.table("users").select("*").eq("phone_number", req.phone_number).execute()
    if not result.data: raise HTTPException(404, f"User {req.phone_number} tidak ditemukan")
    user = result.data[0]
    if user.get("status") == "VIP":
        return {"message": f"{user.get('name','User')} sudah VIP."}
    sb.table("users").update({"status": "VIP"}).eq("phone_number", req.phone_number).execute()
    send_telegram(f"🌟 <b>Upgrade VIP!</b>\n👤 {user.get('name','-')}\n📱 {req.phone_number}")
    return {"message": f"✅ {user.get('name','User')} berhasil diupgrade ke VIP!"}

@app.post("/api/admin/stocks", dependencies=[Depends(require_admin)])
def upsert_stock(s: StockUpsert):
    sb = get_supabase()
    if not sb: raise HTTPException(503, "Supabase tidak tersambung")
    r = sb.table("stocks_data").upsert({
        "ticker": s.ticker.upper(), "price": s.price,
        "volume": s.volume, "updated_at": datetime.now(timezone.utc).isoformat()
    }).execute()
    return {"message": f"Saham {s.ticker.upper()} disimpan.", "data": r.data}

@app.delete("/api/admin/stocks/{ticker}", dependencies=[Depends(require_admin)])
def delete_stock(ticker: str):
    sb = get_supabase()
    if not sb: raise HTTPException(503, "Supabase tidak tersambung")
    sb.table("stocks_data").delete().eq("ticker", ticker.upper()).execute()
    return {"message": f"{ticker.upper()} dihapus."}

@app.post("/api/admin/alerts", dependencies=[Depends(require_admin)])
def create_alert(a: AlertCreate):
    sb = get_supabase()
    if not sb: raise HTTPException(503, "Supabase tidak tersambung")
    r   = sb.table("screener_alerts").insert({
        "ticker": a.ticker.upper(), "price": a.price,
        "indicator_triggered": a.indicator_triggered,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }).execute()
    sent = send_telegram(
        f"🚨 <b>ALERT — {a.ticker.upper()}</b>\n💰 Rp {a.price:,.0f}\n📊 {a.indicator_triggered}",
        chat_id=a.telegram_chat_id
    )
    return {"message": "Alert dibuat", "telegram_sent": sent, "data": r.data}

@app.get("/api/admin/users", dependencies=[Depends(require_admin)])
def list_users():
    sb = get_supabase()
    if not sb: return {"data": []}
    r = sb.table("users").select("*").limit(200).execute()
    return {"data": r.data}
