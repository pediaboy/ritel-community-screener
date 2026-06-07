import os
import time
import random
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
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

# ─────────────────────────────────────────
# DAFTAR LENGKAP TICKER IDX (LQ45 + IDXSMC + populer)
# ─────────────────────────────────────────
IDX_TICKERS = [
    "AALI","ACES","ADHI","ADRO","AGII","AGRO","AHAP","AKRA","ALDO","ALII",
    "ALKA","ALTO","AMFG","AMMN","AMRT","ANTM","ARNA","ASII","ASRI","AUTO",
    "BABP","BACA","BAJA","BANK","BAPA","BBCA","BBHI","BBKP","BBMD","BBNI",
    "BBRI","BBSI","BBTN","BBYB","BCAP","BCIC","BDMN","BEKS","BFIN","BGTG",
    "BHAT","BHIT","BIKA","BIMA","BINA","BIPI","BISI","BJBR","BJTM","BKSL",
    "BLTZ","BLUE","BMRI","BMSR","BMTR","BNBA","BNGA","BNII","BNLI","BPFI",
    "BPII","BRMS","BRNA","BRPT","BSDE","BSSR","BTEK","BTON","BTPN","BUKA",
    "BULL","BUMI","BUVA","BWPT","CAKK","CARR","CASA","CASS","CENT","CFIN",
    "CINT","CITA","CITY","CLPI","CMNP","CMPP","CMRY","CNET","CNTB","COWL",
    "CPIN","CPRI","CSAP","CTRA","CUAN","DADA","DART","DEWA","DFAM","DGNS",
    "DKFT","DLTA","DMAS","DNET","DOID","DPUM","DSFI","DSNG","DUTI","DVLA",
    "DWGL","EKAD","ELSA","ELTY","EMDE","EMTK","ENRG","ERAA","ESSA","ESTD",
    "FAST","FILM","FITT","FLMC","FMII","FOOD","FREN","GAMA","GDST","GEMA",
    "GEMS","GGRM","GIAA","GJTL","GMFI","GOOD","GOTO","GPRA","GZCO","HADE",
    "HEAL","HERO","HKMU","HMSP","HOKI","HOME","HRTA","HRUM","IATA","IBST",
    "ICON","IDEX","IDHM","IDPR","IFII","IGAR","IGST","IIKP","IKAI","IKBI",
    "IMAS","IMJS","IMPC","INAF","INAI","INCF","INCI","INCO","INDF","INDX",
    "INDY","INFO","INKP","INPC","INPP","INRU","INTA","INTD","INTP","IPAC",
    "IPCC","IPCM","IPOL","ISAT","ITMG","JAWA","JECC","JKON","JMAS","JPFA",
    "JRPT","JSMR","JTPE","KAEF","KARW","KBLI","KBLM","KBLV","KDSI","KEEN",
    "KIJA","KINO","KLBF","KMTR","KOPI","KPIG","KRAS","LPCK","LPKR","LPPF",
    "LSIP","LTLS","MAPI","MBAP","MBSS","MDKA","MDKI","MEDC","MEGA","MERK",
    "META","MFIN","MIKA","MIRA","MKPI","MLBI","MLPT","MNCN","MPPA","MRAT",
    "MREI","MSKY","MTDL","MTEL","MTLA","MTOR","MYOH","NCKL","NIKL","NIRO",
    "NISP","NOBU","NRCA","NUSA","NUVF","OCAP","OILS","OKAS","OMRE","PADI",
    "PANR","PANS","PBRX","PBSA","PDES","PGAS","PGJO","PGLI","PGUN","PICO",
    "PJAA","PKPK","PLAN","PLIN","PMMP","PMJS","PNBN","PNGO","PNIN","PNLF",
    "POLA","POLY","POOL","POPE","PPRE","PPRO","PTBA","PTPP","PTRO","PTSN",
    "PUDP","PURA","PWON","PYFA","RAJA","RANC","RDTX","REAL","RELI","RIGS",
    "RIMO","RISE","RODA","ROTI","SAME","SAPX","SCCO","SCMA","SDMU","SGRO",
    "SIDO","SILO","SIMA","SIMP","SINI","SIPD","SKBM","SKLT","SMBR","SMCB",
    "SMDR","SMGR","SMIL","SMRA","SMSM","SOHO","SRIL","SRTG","SSIA","SSMS",
    "SSTM","SUGI","SULI","SWAT","TALF","TARA","TBIG","TBLA","TCID","TELE",
    "TFCO","TINS","TKIM","TLKM","TMPO","TOBA","TOTL","TOTO","TOWR","TPIA",
    "TPMA","TRIM","TRIO","TSPC","TURI","UANG","UCID","ULTJ","UNIC","UNIQ",
    "UNSP","UNTR","UNVR","VICO","VINS","VIVA","VRNA","WAPO","WEGE","WEHA",
    "WIKA","WINS","WMUU","WOOD","WSKT","WTON","YELO","YPAS","ZINC","ZONE",
]

# ─────────────────────────────────────────
# USER-AGENT POOL — random per request
# ─────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]

def random_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.idx.co.id/",
    }

# ─────────────────────────────────────────
# SCRAPING ENGINE — IDX Official API
# ─────────────────────────────────────────
def scrape_idx_batch(tickers: List[str]) -> List[dict]:
    """
    Scrape dari IDX Official API (api.idx.co.id) per batch 50 ticker.
    Lebih reliable dari Yahoo Finance & Investing.com untuk IDX.
    """
    results = []
    batch_size = 50
    
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        codes = ",".join(batch)
        url = f"https://api.idx.co.id/idx/StockData/GetStockQuote?code={codes}&category=EQUITY&start=0&length={batch_size}"
        
        try:
            resp = requests.get(url, headers=random_headers(), timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                stocks = data.get("data", [])
                for s in stocks:
                    try:
                        price_raw = s.get("prev_close_price") or s.get("close_price") or 0
                        vol_raw   = s.get("volume") or 0
                        price = float(str(price_raw).replace(",", "")) if price_raw else 0.0
                        vol   = float(str(vol_raw).replace(",", ""))   if vol_raw   else 0.0
                        if price > 0:
                            results.append({
                                "ticker":     s.get("code", "").strip(),
                                "price":      price,
                                "volume":     vol,
                                "updated_at": datetime.utcnow().isoformat(),
                            })
                    except Exception:
                        continue
            else:
                print(f"[IDX API] Batch {i//batch_size+1} status: {resp.status_code}")
        except Exception as e:
            print(f"[IDX API] Batch error: {e}")
        
        # Anti-block sleep antar batch
        time.sleep(random.uniform(0.5, 1.5))
    
    return results


def scrape_single_investing(ticker: str) -> Optional[dict]:
    """
    Fallback: scrape dari Investing.com kalau IDX API gagal untuk ticker tertentu.
    """
    slug = ticker.lower()
    url  = f"https://www.investing.com/equities/{slug}-historical-data"
    try:
        resp = requests.get(url, headers=random_headers(), timeout=10)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        price_el = soup.select_one('[data-test="instrument-price-last"]')
        if not price_el:
            return None
        price = float(price_el.text.strip().replace(",", "").replace(".", "").replace("\xa0", "")) / 100
        return {
            "ticker":     ticker,
            "price":      price,
            "volume":     0.0,
            "updated_at": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        print(f"[Investing] {ticker} error: {e}")
        return None


def scrape_all_stocks(tickers: List[str] = None) -> dict:
    """
    Main scraping pipeline. Return summary.
    """
    if tickers is None:
        tickers = IDX_TICKERS
    
    print(f"[Scraper] Mulai scrape {len(tickers)} ticker IDX...")
    start_time = time.time()
    
    # Step 1 — IDX Official API (batch, cepat)
    results = scrape_idx_batch(tickers)
    scraped_tickers = {r["ticker"] for r in results}
    
    # Step 2 — Fallback Investing.com untuk yang gagal (max 20 ticker)
    failed = [t for t in tickers if t not in scraped_tickers][:20]
    if failed:
        print(f"[Scraper] IDX API miss {len(failed)} ticker, fallback ke Investing.com...")
        for ticker in failed:
            fallback = scrape_single_investing(ticker)
            if fallback:
                results.append(fallback)
            time.sleep(random.uniform(0.5, 1.5))
    
    # Step 3 — Simpan ke Supabase
    sb = get_supabase()
    saved = 0
    if sb and results:
        for row in results:
            try:
                sb.table("stocks_data").upsert(row, on_conflict="ticker").execute()
                saved += 1
            except Exception as e:
                print(f"[DB] Upsert {row['ticker']} error: {e}")
    
    elapsed = round(time.time() - start_time, 2)
    summary = {
        "total_tickers":  len(tickers),
        "scraped":        len(results),
        "saved_to_db":    saved,
        "elapsed_sec":    elapsed,
        "timestamp":      datetime.utcnow().isoformat(),
    }
    print(f"[Scraper] Done: {summary}")
    return summary


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
# APP
# ─────────────────────────────────────────
app = FastAPI(title="Ritel Community Screener", version="3.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

import pathlib
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
    return FileResponse(str(p)) if p.exists() else {"message": "Ritel Community Screener v3"}

@app.get("/pricing")
def pricing():
    p = static_dir / "pricing.html"
    return FileResponse(str(p)) if p.exists() else {"error": "pricing.html not found"}

@app.get("/admin")
def admin_page():
    p = static_dir / "admin.html"
    return FileResponse(str(p)) if p.exists() else {"error": "admin.html not found"}

# ─────────────────────────────────────────
# ROUTES — PUBLIC
# ─────────────────────────────────────────
@app.get("/api/health")
def health():
    return {"status": "ok", "version": "3.0.0", "ts": datetime.utcnow().isoformat()}

@app.get("/api/stocks")
def get_stocks(limit: int = 100, offset: int = 0):
    sb = get_supabase()
    if not sb:
        return {"data": [], "error": "Supabase belum terhubung"}
    try:
        r = sb.table("stocks_data").select("*").order("volume", desc=True).range(offset, offset + limit - 1).execute()
        return {"data": r.data, "total": len(r.data), "live": True}
    except Exception as e:
        return {"data": [], "error": str(e)}

@app.get("/api/alerts")
def get_alerts(limit: int = 20):
    sb = get_supabase()
    if not sb:
        return {"data": []}
    try:
        r = sb.table("screener_alerts").select("*").order("timestamp", desc=True).limit(limit).execute()
        return {"data": r.data}
    except Exception as e:
        return {"data": [], "error": str(e)}

@app.get("/api/tickers")
def get_tickers():
    return {"tickers": IDX_TICKERS, "total": len(IDX_TICKERS)}

# ─────────────────────────────────────────
# ROUTES — SCRAPING (CRON + MANUAL)
# ─────────────────────────────────────────
@app.post("/api/cron-scrape")
def cron_scrape(background_tasks: BackgroundTasks, x_admin_secret: str = Header(...)):
    """Endpoint untuk cron job atau trigger manual dari admin panel."""
    if x_admin_secret != ADMIN_SECRET:
        raise HTTPException(403, "Admin secret salah.")
    background_tasks.add_task(scrape_all_stocks, IDX_TICKERS)
    return {"message": f"Scraping {len(IDX_TICKERS)} ticker IDX dimulai di background!", "ts": datetime.utcnow().isoformat()}

@app.post("/api/admin/force-scrape", dependencies=[Depends(require_admin)])
def force_scrape(background_tasks: BackgroundTasks):
    """Alias force-scrape untuk admin panel."""
    background_tasks.add_task(scrape_all_stocks, IDX_TICKERS)
    return {"message": f"Force scrape {len(IDX_TICKERS)} ticker dimulai!", "ts": datetime.utcnow().isoformat()}

@app.get("/api/admin/scrape-status", dependencies=[Depends(require_admin)])
def scrape_status():
    sb = get_supabase()
    if not sb:
        return {"count": 0}
    r = sb.table("stocks_data").select("ticker, updated_at").order("updated_at", desc=True).limit(1).execute()
    count = sb.table("stocks_data").select("ticker", count="exact").execute()
    return {
        "total_in_db":  count.count if hasattr(count, "count") else 0,
        "latest_update": r.data[0] if r.data else None,
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
        f"👤 {user.get('name','-')}\n"
        f"📱 {req.phone_number}\n"
        f"🕒 {datetime.utcnow().strftime('%d/%m/%Y %H:%M')} UTC"
    )
    return {"message": f"✅ {user.get('name','User')} berhasil diupgrade ke VIP!"}

@app.post("/api/admin/stocks", dependencies=[Depends(require_admin)])
def upsert_stock(s: StockUpsert):
    sb = get_supabase()
    if not sb:
        raise HTTPException(503, "Supabase tidak tersambung")
    payload = {"ticker": s.ticker.upper(), "price": s.price, "volume": s.volume, "updated_at": datetime.utcnow().isoformat()}
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
    payload = {"ticker": a.ticker.upper(), "price": a.price, "indicator_triggered": a.indicator_triggered, "timestamp": datetime.utcnow().isoformat()}
    r = sb.table("screener_alerts").insert(payload).execute()
    msg = (
        f"🚨 <b>ALERT — {a.ticker.upper()}</b>\n"
        f"💰 Rp {a.price:,.0f}\n"
        f"📊 {a.indicator_triggered}\n"
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
