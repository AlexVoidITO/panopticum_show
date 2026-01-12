from sqlmodel import SQLModel, Field
from typing import Optional


class PointBase(SQLModel):
    home_num : str
    volts : float
    ampers : float
    power : float
    resistance : float

class Point(PointBase, table=True):
    home_id: int = Field(default=None, nullable=False, primary_key=True)


class PointCreate(PointBase):
    pass

class PointUpdate(SQLModel):
    home_num : Optional[str] = None
    volts : Optional[float] = None
    ampers : Optional[float] = None
    power : Optional[float] = None
    resistance : Optional[float] = None