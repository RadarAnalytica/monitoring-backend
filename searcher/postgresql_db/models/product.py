from sqlalchemy import String, Integer
from sqlalchemy.orm import mapped_column, Mapped

from ..base import Base


class Product(Base):
    __tablename__ = "products"

    wb_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    brand: Mapped[str] = mapped_column(String, nullable=True)
    brandId: Mapped[int] = mapped_column(Integer, nullable=True)
    supplier: Mapped[str] = mapped_column(String, nullable=True)
    supplierId: Mapped[int] = mapped_column(Integer, nullable=True)
    entity: Mapped[str] = mapped_column(String, nullable=True)
