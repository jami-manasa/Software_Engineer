from app.core.database import engine
from app.models.raw_data import Base

Base.metadata.create_all(bind=engine)
