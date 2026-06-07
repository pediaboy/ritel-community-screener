import os
import time
import random
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Depends, Header, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import pathlib

# ─────────────────────────────────────────
# ENV
# ─────────────────────────────────────────
SUPABASE_URL       = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY       = os.getenv("SUPABASE_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
ADMIN_SECRET       = os.getenv("ADMIN_SECRET", "pedia123")

# ─────────────────────────────────────────
# DAFTAR LENGKAP TICKER IDX
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

# ─────────────────────────────────────────
# SUPABASE
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
# TELEGRAM
# ─────────────────────────────────────────
def send_telegram(message: str, chat_id: str = None) -> bool:
    target = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or not target:
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": target, "text": message, "parse_mode": "HTML"},
            timeout=10
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[Telegram] Error: {e}")
        return False

# ─────────────────────────────────────────
# SCRAPING ENGINE — yfinance batch (proven, no Cloudflare block)
# ─────────────────────────────────────────
def scrape_all_stocks(tickers: List[str] = None) -> dict:
    """
    Scrape harga terbaru IDX via yfinance batch download.
    Batch 100 ticker per call, ~1-3 detik per batch.
    """
    if tickers is None:
        tickers = IDX_TICKERS

    print(f"[Scraper] Mulai scrape {len(tickers)} ticker IDX via yfinance...")
    start_time = time.time()

    batch_size = 100
    all_rows   = []

    for i in range(0, len(tickers), batch_size):
        batch   = tickers[i : i + batch_size]
        symbols = [f"{t}.JK" for t in batch]

        try:
            data = yf.download(
                " ".join(symbols),
                period="2d",           # ambil 2 hari supaya selalu ada data walau market belum buka
                interval="1d",
                auto_adjust=True,
                progress=False,
                threads=True,
            )

            if data.empty:
                print(f"[Scraper] Batch {i//batch_size+1} kosong")
                continue

            # Ambil baris terakhir yang valid
            close  = data["Close"].iloc[-1]
            volume = data["Volume"].iloc[-1]
            now_ts = datetime.utcnow().isoformat()

            for ticker in batch:
                sym   = f"{ticker}.JK"
                price = close.get(sym)
                vol   = volume.get(sym)

                # Skip NaN
                if price is None or str(price) == "nan":
                    continue

                all_rows.append({
                    "ticker":     ticker,
                    "price":      round(float(price), 2),
                    "volume":     float(vol) if vol and str(vol) != "nan" else 0.0,
                    "updated_at": now_ts,
                })

            print(f"[Scraper] Batch {i//batch_size+1}: {len(all_rows)} rows so far")

        except Exception as e:
            print(f"[Scraper] Batch {i//batch_size+1} error: {e}")

        # Jeda antar batch — jangan spam Yahoo
        time.sleep(random.uniform(1.0, 2.0))

    # ── Simpan ke Supabase ──
    sb    = get_supabase()
    saved = 0
    if sb and all_rows:
        for row in all_rows:
            try:
                sb.table("stocks_data").upsert(row, on_conflict="ticker").execute()
                saved += 1
            except Exception as e:
                print(f"[DB] Upsert {row['ticker']} error: {e}")

    elapsed = round(time.time() - start_time, 2)
    summary = {
        "total_tickers": len(tickers),
        "scraped":       len(all_rows),
        "saved_to_db":   saved,
        "elapsed_sec":   elapsed,
        "timestamp":     datetime.utcnow().isoformat(),
    }
    print(f"[Scraper] Done: {summary}")
    return summary

# ─────────────────────────────────────────
# APP
# ─────────────────────────────────────────
app = FastAPI(title="Ritel Community Screener", version="3.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

static_dir = pathlib.Path(__file__).parent.parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

@app.exception_handler(Exception)
async def global_error(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"error": str(exc)})

# ─────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────
def require_admin(x_admin_secret: str = Header(...)):
    if x_admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Admin secret salah.")

# ─────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────
class UpgradeUserReq(BaseModel):
    phone_number: str

class StockUpsert(BaseModel):
    ticker: str
    price: float
    volume: float

class AlertCreate(BaseModel):
    ticker: str
    price: float
    indicator_triggered: str
    telegram_chat_id: Optional[str] = None

# ─────────────────────────────────────────
# ROUTES — FRONTEND
# ─────────────────────────────────────────
@app.get("/")
def index():
    p = static_dir / "index.html"
    return FileResponse(str(p)) if p.exists() else {"message": "Ritel Community Screener v3.1"}

@app.get("/pricing")
def pricing():
    p = static_dir / "pricing.html"
    return FileResponse(str(p)) if p.exists() else {"error": "pricing.html not found"}

@app.get("/admin")
def admin_page():
    p = static_dir / "admin.html"
    return FileResponse(str(p)) if p.exists() else {"error": "admin.html not found"}

# ─────────────────────────────────────────
# ROUTES — PUBLIC API
# ─────────────────────────────────────────
@app.get("/api/health")
def health():
    return {"status": "ok", "version": "3.1.0", "ts": datetime.utcnow().isoformat()}

@app.get("/api/stocks")
def get_stocks(limit: int = 100, offset: int = 0):
    sb = get_supabase()
    if not sb:
        return {"data": [], "error": "Supabase belum terhubung"}
    try:
        r = (sb.table("stocks_data")
               .select("*")
               .order("volume", desc=True)
               .range(offset, offset + limit - 1)
               .execute())
        return {"data": r.data, "total": len(r.data), "live": True}
    except Exception as e:
        return {"data": [], "error": str(e)}

@app.get("/api/alerts")
def get_alerts(limit: int = 20):
    sb = get_supabase()
    if not sb:
        return {"data": []}
    try:
        r = (sb.table("screener_alerts")
               .select("*")
               .order("timestamp", desc=True)
               .limit(limit)
               .execute())
        return {"data": r.data}
    except Exception as e:
        return {"data": [], "error": str(e)}

@app.get("/api/tickers")
def get_tickers():
    return {"tickers": IDX_TICKERS, "total": len(IDX_TICKERS)}

# ─────────────────────────────────────────
# ROUTES — SCRAPING
# ─────────────────────────────────────────
@app.post("/api/cron-scrape")
def cron_scrape(background_tasks: BackgroundTasks, x_admin_secret: str = Header(...)):
    """Cron job / trigger manual dari admin panel."""
    if x_admin_secret != ADMIN_SECRET:
        raise HTTPException(403, "Admin secret salah.")
    background_tasks.add_task(scrape_all_stocks, IDX_TICKERS)
    return {
        "message": f"Scraping {len(IDX_TICKERS)} ticker IDX dimulai!",
        "ts": datetime.utcnow().isoformat(),
    }

@app.post("/api/admin/force-scrape", dependencies=[Depends(require_admin)])
def force_scrape(background_tasks: BackgroundTasks):
    background_tasks.add_task(scrape_all_stocks, IDX_TICKERS)
    return {"message": f"Force scrape {len(IDX_TICKERS)} ticker dimulai!", "ts": datetime.utcnow().isoformat()}

@app.get("/api/admin/scrape-status", dependencies=[Depends(require_admin)])
def scrape_status():
    sb = get_supabase()
    if not sb:
        return {"count": 0}
    latest = (sb.table("stocks_data")
                .select("ticker, updated_at")
                .order("updated_at", desc=True)
                .limit(1)
                .execute())
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
    if not sb:
        raise HTTPException(503, "Supabase tidak tersambung")
    result = sb.table("users").select("*").eq("phone_number", req.phone_number).execute()
    if not result.data:
        raise HTTPException(404, f"User {req.phone_number} tidak ditemukan")
    user = result.data[0]
    if user.get("status") == "VIP":
        return {"message": f"{user.get('name','User')} sudah VIP.", "user": user}
    sb.table("users").update({"status": "VIP"}).eq("phone_number", req.phone_number).execute()
    send_telegram(
        f"🌟 <b>Upgrade VIP Berhasil!</b>\n"
        f"👤 {user.get('name','-')}\n📱 {req.phone_number}\n"
        f"🕒 {datetime.utcnow().strftime('%d/%m/%Y %H:%M')} UTC"
    )
    return {"message": f"✅ {user.get('name','User')} berhasil diupgrade ke VIP!"}

@app.post("/api/admin/stocks", dependencies=[Depends(require_admin)])
def upsert_stock(s: StockUpsert):
    sb = get_supabase()
    if not sb:
        raise HTTPException(503, "Supabase tidak tersambung")
    payload = {
        "ticker":     s.ticker.upper(),
        "price":      s.price,
        "volume":     s.volume,
        "updated_at": datetime.utcnow().isoformat(),
    }
    r = sb.table("stocks_data").upsert(payload).execute()
    return {"message": f"Saham {s.ticker.upper()} disimpan.", "data": r.data}

@app.delete("/api/admin/stocks/{ticker}", dependencies=[Depends(require_admin)])
def delete_stock(ticker: str):
    sb = get_supabase()
    if not sb:
        raise HTTPException(503, "Supabase tidak tersambung")
    sb.table("stocks_data").delete().eq("ticker", ticker.upper()).execute()
    return {"message": f"Saham {ticker.upper()} dihapus."}

@app.post("/api/admin/alerts", dependencies=[Depends(require_admin)])
def create_alert(a: AlertCreate):
    sb = get_supabase()
    if not sb:
        raise HTTPException(503, "Supabase tidak tersambung")
    payload = {
        "ticker":               a.ticker.upper(),
        "price":                a.price,
        "indicator_triggered":  a.indicator_triggered,
        "timestamp":            datetime.utcnow().isoformat(),
    }
    r   = sb.table("screener_alerts").insert(payload).execute()
    msg = (
        f"🚨 <b>ALERT — {a.ticker.upper()}</b>\n"
        f"💰 Rp {a.price:,.0f}\n📊 {a.indicator_triggered}\n"
        f"🕒 {datetime.utcnow().strftime('%d/%m/%Y %H:%M')} UTC\n"
        f"<i>Ritel Community.ID</i>"
    )
    sent = send_telegram(msg, chat_id=a.telegram_chat_id)
    return {"message": "Alert dibuat", "telegram_sent": sent, "data": r.data}

@app.get("/api/admin/users", dependencies=[Depends(require_admin)])
def list_users():
    sb = get_supabase()
    if not sb:
        return {"data": []}
    r = sb.table("users").select("*").limit(200).execute()
    return {"data": r.data}
