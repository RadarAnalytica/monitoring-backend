from sqlalchemy import String, Integer, Float, BigInteger
from sqlalchemy.orm import mapped_column, Mapped

from ..base import Base


class City(Base):
    __tablename__ = "cities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, unique=True)
    dest: Mapped[int] = mapped_column(BigInteger, unique=True)
    lat: Mapped[float] = mapped_column(Float, nullable=True)
    lon: Mapped[float] = mapped_column(Float, nullable=True)