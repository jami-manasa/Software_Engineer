from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.core.redis_client import r
from app.core.database import SessionLocal
from app.models.raw_data import RawMarketData
import yfinance as yf
import json
from app.services.producer import produce_price_event

router = APIRouter()

# DB dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/prices/latest")
async def get_latest_price(
    symbol: str,
    provider: str = "yfinance",
    db: Session = Depends(get_db)
):
    key = f"{provider}:{symbol}"

    # 1. Try Redis
    if r.exists(key):
        return json.loads(r.get(key))

    # 2. Fetch from yfinance
    ticker = yf.Ticker(symbol)
    data = ticker.history(period="1d")
    if data.empty:
        return {"error": "Symbol not found or no data"}

    price = float(data["Close"].iloc[-1])  # ✅ convert np.float64 to float
    ts = data.index[-1].to_pydatetime()    # ✅ convert pandas.Timestamp to datetime


    response = {
        "symbol": symbol,
        "price": round(price, 2),
        "timestamp": str(ts),
        "provider": provider
    }

    # 3. Save to Redis
    r.setex(key, 300, json.dumps(response))

    # 4. Save to PostgreSQL
    new_data = RawMarketData(
        symbol=symbol,
        price=price,
        timestamp=ts,
        provider=provider
    )
    db.add(new_data)
    db.commit()
    produce_price_event(response)

    return response
