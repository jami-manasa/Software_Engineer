from sqlalchemy import Column, Integer, String, Float, DateTime
from app.core.database import Base
from datetime import datetime

class RawMarketData(Base):
    __tablename__ = "raw_market_data"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True)
    price = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)
    provider = Column(String)
