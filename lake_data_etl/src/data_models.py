from sqlmodel import SQLModel, Field
from typing import Optional
import datetime

from sqlmodel import Session, select

from shapely import Polygon

from dataclasses import dataclass

class Lake(SQLModel, table=True):
    __tablename__ = "lakes"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    lake_name: str
    latitude: float = None
    longitude: float = None
    nearby_city_name: str = None
    state_or_province: str = None
    country: str = None
    nearby_city_latitude: float = None
    nearby_city_longitude: float = None
    max_depth_m: float = None
    surface_area_m2: float = None


# class LakeShape(SQLModel, table=True):
#     __tablename__ = "lake_geometry"

#     permanent_: str = None 
#     fdate: str = None
#     resolution: str = int
#     gnis_id: str = Field(primary_key=True)
#     gnis_name: str = None
#     areasqkm: float = None
#     elevation: float = None
#     reachcode: str = None
#     ftype: int = None
#     fcode: int = None
#     visibility: int = None
#     SHAPE_Leng: float = None
#     SHAPE_Area: float = None
#     ObjectID: int = None
#     geometry: tuple[tuple[float]] = Field()

    
class WeatherByDay(SQLModel, table=True):
    __tablename__ = "weather_by_day"
    
    date: datetime.date = Field(primary_key=True)
    latitude: float = Field(primary_key=True)
    longitude: float = Field(primary_key=True)
    nearby_city_name: str
    state_or_province: str 
    country: str
    max_temp_c: float
    min_temp_c: float
    avg_temp_c: float
    max_wind_kph: float
    total_precip_mm: float
    avg_visibility_km: float
    avg_humidity: float
    uv: float
    

@dataclass  
class Location():
    latitude: float = None
    longitude: float = None
    nearby_city_name: str = None
    state_or_province: str = None
    country: str = None
