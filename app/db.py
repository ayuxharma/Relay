from sqlalchemy.ext.asyncio import create_async_engine

from app.config import settings


engine = create_async_engine(
    settings.database_url.get_secret_value(),
    pool_pre_ping=True,
    pool_timeout=3,
    connect_args={"connect_timeout": 3},
)