import os
import requests
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from supabase import create_client, Client

# ─────────────────────────────────────────
# ENV
# ─────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://ubsowwkgpooexrmwdpii.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "pedia123")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─────────────────────────────────────────
# APP
# ─────────────────────────────────────────
app = FastAPI(title="Ritel Community Screener API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static frontend
app.mount("/static", StaticFiles(directory="static"), name="static")

# ─────────────────────────────────────────
# TELEGRAM INDEPENDENT BOT (no Base44)
# ─────────────────────────────────────────
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def send_telegram_alert(message: str, chat_id: str = None):
    """Send alert directly to Telegram API using requests — independent of Base44."""
    target = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or not target:
        print("[Telegram] Token or chat_id missing — alert skipped.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": target,
        "text": message,
        "parse_mode": "HTML",
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[Telegram] Error sending alert: {e}")
        return False

# ─────────────────────────────────────────
# AUTH HELPER
# ─────────────────────────────────────────
def require_admin(x_admin_secret: str = Header(...)):
    if x_admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Invalid admin secret")

# ─────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────
class UpgradeUserRequest(BaseModel):
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

class AnnouncementCreate(BaseModel):
    title: str
    body: str

class FAQItem(BaseModel):
    question: str
    answer: str

# ─────────────────────────────────────────
# ROUTES — PUBLIC
# ─────────────────────────────────────────
@app.get("/")
def root():
    return FileResponse("static/index.html")

@app.get("/pricing")
def pricing_page():
    return FileResponse("static/pricing.html")

@app.get("/admin")
def admin_page():
    return FileResponse("static/admin.html")

@app.get("/api/stocks")
def get_stocks(limit: int = 50, offset: int = 0):
    """Public endpoint: top 10 free, rest requires VIP (enforced on frontend)."""
    result = (
        supabase.table("stocks_data")
        .select("*")
        .order("volume", desc=True)
        .range(offset, offset + limit - 1)
        .execute()
    )
    return {"data": result.data, "total": len(result.data)}

@app.get("/api/alerts")
def get_alerts(limit: int = 20):
    result = (
        supabase.table("screener_alerts")
        .select("*")
        .order("timestamp", desc=True)
        .limit(limit)
        .execute()
    )
    return {"data": result.data}

@app.get("/api/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}

# ─────────────────────────────────────────
# ROUTES — ADMIN
# ─────────────────────────────────────────
@app.post("/api/admin/upgrade-user", dependencies=[Depends(require_admin)])
def upgrade_user(req: UpgradeUserRequest):
    """Upgrade a user to VIP by phone number."""
    result = (
        supabase.table("users")
        .select("id, name, phone_number, status")
        .eq("phone_number", req.phone_number)
        .execute()
    )
    if not result.data:
        raise HTTPException(status_code=404, detail=f"User with phone {req.phone_number} not found")
    user = result.data[0]
    if user["status"] == "VIP":
        return {"message": f"User {user['name']} is already VIP.", "user": user}
    supabase.table("users").update({"status": "VIP"}).eq("phone_number", req.phone_number).execute()
    # Notify via Telegram
    send_telegram_alert(
        f"🌟 <b>User Upgraded to VIP</b>\n"
        f"Nama: {user['name']}\n"
        f"No HP: {req.phone_number}\n"
        f"Waktu: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
    )
    return {"message": f"User {user['name']} upgraded to VIP successfully.", "user": user}

@app.post("/api/admin/stocks", dependencies=[Depends(require_admin)])
def upsert_stock(stock: StockUpsert):
    """CMS: Add or update a stock entry."""
    payload = {
        "ticker": stock.ticker.upper(),
        "price": stock.price,
        "volume": stock.volume,
        "updated_at": datetime.utcnow().isoformat(),
    }
    result = supabase.table("stocks_data").upsert(payload).execute()
    return {"message": "Stock upserted", "data": result.data}

@app.delete("/api/admin/stocks/{ticker}", dependencies=[Depends(require_admin)])
def delete_stock(ticker: str):
    supabase.table("stocks_data").delete().eq("ticker", ticker.upper()).execute()
    return {"message": f"Stock {ticker.upper()} deleted"}

@app.post("/api/admin/alerts", dependencies=[Depends(require_admin)])
def create_alert(alert: AlertCreate):
    """CMS: Manually trigger a screener alert and send Telegram notification."""
    payload = {
        "ticker": alert.ticker.upper(),
        "price": alert.price,
        "indicator_triggered": alert.indicator_triggered,
        "timestamp": datetime.utcnow().isoformat(),
    }
    result = supabase.table("screener_alerts").insert(payload).execute()
    # Send Telegram alert
    msg = (
        f"🚨 <b>STOCK ALERT — {alert.ticker.upper()}</b>\n"
        f"💰 Harga: Rp {alert.price:,.0f}\n"
        f"📊 Indikator: {alert.indicator_triggered}\n"
        f"🕒 Waktu: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        f"<i>Ritel Community.ID</i>"
    )
    sent = send_telegram_alert(msg, chat_id=alert.telegram_chat_id)
    return {"message": "Alert created", "telegram_sent": sent, "data": result.data}

@app.post("/api/admin/force-scrape", dependencies=[Depends(require_admin)])
def force_scrape():
    """Trigger a manual scrape run (placeholder — extend with real scraper logic)."""
    # In production: call your scraper module here
    return {"message": "Force scrape triggered", "timestamp": datetime.utcnow().isoformat()}

@app.get("/api/admin/users", dependencies=[Depends(require_admin)])
def list_users(limit: int = 100):
    result = supabase.table("users").select("*").limit(limit).execute()
    return {"data": result.data, "total": len(result.data)}
