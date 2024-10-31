from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import NullPool
from settings import POSTGRES_CONFIG, logger



DB_URL = (
    f"{POSTGRES_CONFIG['driver']}://"
    f"{POSTGRES_CONFIG['username']}:"
    f"{POSTGRES_CONFIG['password']}@"
    f"{POSTGRES_CONFIG['host']}:"
    f"{POSTGRES_CONFIG['port']}"
    f"{'/' + POSTGRES_CONFIG['database'] if POSTGRES_CONFIG.get('database') else ''}"
)

engine = create_async_engine(DB_URL, poolclass=NullPool)
async_session_maker = async_sessionmaker(bind=engine, expire_on_commit=False)


Base = declarative_base()
