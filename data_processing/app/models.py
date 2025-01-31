from sqlalchemy import Column, String, DECIMAL, ForeignKey, TIMESTAMP
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


class Company(Base):
    __tablename__ = "companies"

    id = Column(String(40), primary_key=True)
    name = Column(String(130), nullable=False)
    charges = relationship("Charge", back_populates="company", cascade="all, delete")

class Charge(Base):
    __tablename__ = "charges"

    id = Column(String(40), primary_key=True)
    company_id = Column(String(24), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    amount = Column(DECIMAL(16, 2), nullable=False)
    status = Column(String(30), nullable=False)
    created_at = Column(TIMESTAMP, nullable=False)
    updated_at = Column(TIMESTAMP, nullable=True)

    company = relationship("Company", back_populates="charges")

def init_db(engine):
    try:
        Base.metadata.create_all(bind=engine)
        print("✅ Base de datos y tablas creadas correctamente.")
    except Exception as e:
        print(e)
        return