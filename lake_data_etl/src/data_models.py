from sqlmodel import SQLModel, Field
from typing import Optional
import datetime

from shapely import Polygon, MultiPolygon
from shapely.geometry import mapping
from sqlmodel import Session, select

from shapely import Polygon

from dataclasses import dataclass

@dataclass
class LakeGeometry:
    # permanent_: str = None 
    fdate: str = None
    resolution: str = int
    gnis_id: str = Field(primary_key=True)
    gnis_name: str = None
    areasqkm: float = None
    elevation: float = None
    reachcode: str = None
    ftype: int = None
    fcode: int = None
    visibility: int = None
    shape_length: float = None
    shape_area: float = None
    object_id: int = None
    geometry: str = None # jsonb type in postgres #list[list[list[float]]]
    boundary: list[list[float]] = None
    bounds: list[float] = None
    bounding_box: tuple[tuple[float]] = None
    bounding_box_centroid: tuple[float] = None

    def __post_init__(self):
        polygon = MultiPolygon(
                (Polygon(points) for points in self.geometry) #json.loads()
            )
        
        boundary = mapping(polygon.boundary)['coordinates']
        if len(boundary) != 1:
            # raise Exception(f"Boundary should only be 1 continuous polygon: {boundary[1:]} length: {len(boundary)}")
            self.boundary = []
        else:
            self.boundary = boundary[0]
        self.bounds = polygon.bounds

        min_lat, min_long, max_lat, max_long = self.bounds

        self.bounding_box = ((min_lat, min_long), (min_lat, max_long), (max_lat, max_long), (max_lat, min_long))

        self.centroid = list(polygon.centroid.coords)[0]

        self.bounding_box_centroid = ( (min_lat + max_lat) / 2, (min_long + max_long) / 2 )  # Probably should do coordinate math with a globe projection

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
