import os
import requests
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException, Depends, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

# ─────────────────────────────────────────
# ENV — Railway inject otomatis
# ─────────────────────────────────────────
SUPABASE_URL      = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY      = os.getenv("SUPABASE_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "")
ADMIN_SECRET      = os.getenv("ADMIN_SECRET", "pedia123")

# ─────────────────────────────────────────
# SUPABASE CLIENT — lazy init
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
# TELEGRAM — independen, pure requests
# ─────────────────────────────────────────
def send_telegram(message: str, chat_id: str = None) -> bool:
    """Kirim pesan langsung ke Telegram API tanpa library tambahan."""
    target = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or not target:
        print("[Telegram] Token atau chat_id belum diset.")
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
        print(f"[Telegram] Gagal kirim: {e}")
        return False

# ─────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────
app = FastAPI(title="Ritel Community Screener", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files — serve frontend
import pathlib
static_dir = pathlib.Path(__file__).parent.parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# ─────────────────────────────────────────
# GLOBAL ERROR HANDLER
# ─────────────────────────────────────────
@app.exception_handler(Exception)
async def global_error(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"error": str(exc), "path": str(request.url)})

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
# ROUTES — FRONTEND SERVE
# ─────────────────────────────────────────
@app.get("/")
def index():
    p = static_dir / "index.html"
    return FileResponse(str(p)) if p.exists() else {"message": "Ritel Community Screener API v2"}

@app.get("/pricing")
def pricing():
    p = static_dir / "pricing.html"
    return FileResponse(str(p)) if p.exists() else {"error": "pricing.html not found"}

@app.get("/admin")
def admin():
    p = static_dir / "admin.html"
    return FileResponse(str(p)) if p.exists() else {"error": "admin.html not found"}

# ─────────────────────────────────────────
# ROUTES — PUBLIC API
# ─────────────────────────────────────────
@app.get("/api/health")
def health():
    return {"status": "ok", "ts": datetime.utcnow().isoformat(), "supabase": bool(SUPABASE_URL)}

@app.get("/api/stocks")
def get_stocks(limit: int = 50, offset: int = 0):
    sb = get_supabase()
    if not sb:
        return {"data": [], "error": "Supabase belum terhubung"}
    try:
        r = sb.table("stocks_data").select("*").order("volume", desc=True).range(offset, offset + limit - 1).execute()
        return {"data": r.data}
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
        raise HTTPException(404, f"User dengan HP {req.phone_number} tidak ditemukan")
    user = result.data[0]
    if user.get("status") == "VIP":
        return {"message": f"{user.get('name','User')} sudah VIP.", "user": user}
    sb.table("users").update({"status": "VIP"}).eq("phone_number", req.phone_number).execute()
    send_telegram(
        f"🌟 <b>Upgrade VIP Berhasil!</b>\n"
        f"👤 Nama: {user.get('name', '-')}\n"
        f"📱 HP: {req.phone_number}\n"
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
        f"💰 Harga: Rp {a.price:,.0f}\n"
        f"📊 Indikator: {a.indicator_triggered}\n"
        f"🕒 {datetime.utcnow().strftime('%d/%m/%Y %H:%M')} UTC\n\n"
        f"<i>Ritel Community.ID</i>"
    )
    sent = send_telegram(msg, chat_id=a.telegram_chat_id)
    return {"message": "Alert dibuat", "telegram_sent": sent, "data": r.data}

@app.post("/api/admin/force-scrape", dependencies=[Depends(require_admin)])
def force_scrape():
    # TODO: integrasikan scraper IDX di sini
    return {"message": "Force scrape dipicu!", "ts": datetime.utcnow().isoformat()}

@app.get("/api/admin/users", dependencies=[Depends(require_admin)])
def list_users():
    sb = get_supabase()
    if not sb:
        return {"data": []}
    r = sb.table("users").select("*").limit(200).execute()
    return {"data": r.data}
