

# Pull lake data from usgs files

# https://www.usgs.gov/national-hydrography/access-national-hydrography-products
# https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Hydrography/NHD/State/Shape/

import boto3
import os
import json
# import us
import zipfile
import io
import geopandas as gpd
import pandas as pd
from simpledbf import Dbf5
from typing import Optional

from sqlmodel import SQLModel, Field
from database import engine, sqlalchemy_engine

from shapely import Polygon, MultiPolygon
from shapely.geometry import mapping

from data_models import LakeGeometry

# SQLModel.metadata.create_all(engine)

from dataclasses import dataclass
# 

def get_jdbc_options():
    from database import db_endpoint, db_password, db_username
    jdbc_url = f"jdbc:postgresql://{db_endpoint}:5432/"

    # logger.debug(jdbc_url)

    return {
        "url": jdbc_url,
        "user": db_username,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

# spark = SparkSession.builder.master("local[1]")\
#     .getOrCreate()

s3 = boto3.client('s3')

s3_bucket, s3_prefix = 'prd-tnm', 'StagedProducts/Hydrography/NHD/State/Shape/'
local_data_dir = 'raw_data'
# ls_resp = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

states = ['Minnesota'] #us.states.STATES_AND_TERRITORIES


def load_geometry_data():
    for state in states:
        filename = f"NHD_H_{state}_State_Shape.zip"
        s3_key = s3_prefix + filename

        local_filename = os.path.join(local_data_dir, filename) 

        if not os.path.exists(local_filename):
            s3.download_file(s3_bucket, s3_key, local_filename)

        with open(local_filename, 'rb') as f:
            zip_bytes = f.read()

        waterbody_dbf_filename_in_zip = "Shape/NHDWaterbody.dbf"
        local_waterbody_dbf_filename = os.path.join("raw_data", f"{state}_NHDWaterbody.dbf")

        waterbody_shp_filename_in_zip = "Shape/NHDWaterbody.shp"
        local_waterbody_shp_filename = os.path.join("raw_data", f"{state}_NHDWaterbody.shp")

        waterbody_shx_filename_in_zip = "Shape/NHDWaterbody.shx"
        local_waterbody_shx_filename = os.path.join("raw_data", f"{state}_NHDWaterbody.shx")

        
        with zipfile.ZipFile(io.BytesIO(zip_bytes), 'r') as z: # 
            with z.open(waterbody_dbf_filename_in_zip, 'r') as zip_f:
                with open(local_waterbody_dbf_filename, 'wb') as f:
                    f.write(zip_f.read())

                # waterbodies: pd.DataFrame = Dbf5(local_waterbody_dbf_filename).to_dataframe()
                # lakes = waterbodies.dropna(subset=['gnis_name']) 
                # print(lakes.head())
                # print(waterbodies['gnis_name'])
            if not os.path.exists(local_waterbody_shp_filename):
                with z.open(waterbody_shp_filename_in_zip, 'r') as zip_f:
                    with open(local_waterbody_shp_filename, 'wb') as f:
                        f.write(zip_f.read())
            
            if not os.path.exists(local_waterbody_shx_filename):
                with z.open(waterbody_shx_filename_in_zip, 'r') as zip_f:
                    with open(local_waterbody_shx_filename, 'wb') as f:
                        f.write(zip_f.read())

        shp = gpd.read_file(local_waterbody_shp_filename)

        def get_coords(polygon):
            coords = mapping(polygon)['coordinates']
            
            if isinstance(coords, list):
                coords = coords[0]
                return json.dumps(coords)
            if isinstance(coords, tuple) and isinstance(coords[0], tuple) and isinstance(coords[0][0], tuple) and isinstance(coords[0][0][0], float):
                return json.dumps(coords)
            else:
                raise Exception(f"bad type for {type(coords)} {coords}")
            
        # def get_coords(polygon: Polygon):
        #     poly = polygon.__geo_interface__['coordinates']            
        #     return json.dumps(poly)
            
            
        shp = gpd.read_file(local_waterbody_shp_filename)

        lakes = shp.dropna(subset=['gnis_name']) # Filter out water bodies with no name

        lakes['geometry'] = lakes['geometry'].apply(get_coords)

        # print(lakes )

        lakes[[
               'boundary', 'bounds', 'bounding_box', 'bounding_box_centroid',
               'latitude', 'longitude',
               'min_lat', 'min_long', 'max_lat', 'max_long'
               ]] = lakes.apply(get_bounding_box, axis=1)


        from sqlalchemy.dialects import postgresql

        lakes.to_sql(name='waterbody_geometry_temp', con=sqlalchemy_engine, if_exists='append', index=False)


def get_bounding_box(row):
        geometry_str = row["geometry"]
        geometry = json.loads(geometry_str)
        # print(geometry)

        polygon = MultiPolygon(
            (Polygon(points) for points in geometry) #
        )
        
        boundary = mapping(polygon.boundary)['coordinates']
        if len(boundary) != 1:
            # raise Exception(f"Boundary should only be 1 continuous polygon: {boundary[1:]} length: {len(boundary)}")
            boundary = []
        else:
            boundary = boundary[0]
        bounds = polygon.bounds

        min_long, min_lat, max_long, max_lat = bounds

        bounding_box = json.dumps(((min_lat, min_long), (min_lat, max_long), (max_lat, max_long), (max_lat, min_long)))

        centroid = list(polygon.centroid.coords)[0]

        bounding_box_centroid = json.dumps(( (min_lat + max_lat) / 2, (min_long + max_long) / 2 ))  # Probably should do coordinate math with a globe projection

        latitude = (min_lat + max_lat) / 2
        longitude = (min_long + max_long) / 2

        return pd.Series([json.dumps(boundary), bounds, bounding_box, bounding_box_centroid, latitude, longitude, min_lat, min_long, max_lat, max_long])

if __name__ == "__main__":
    load_geometry_data()

#postgres script
"""
insert INTO water_bodies 
with geo as (
	select 
		*,
		bounding_box_centroid[1] as longitude , 
		bounding_box_centroid[2] as latitude,
		replace(trim(replace(lower(gnis_name), 'lake', '')), ' ', '_') as lake_name 
	from public.lake_geometry_test
)
,
lake as (
	select * from lakes
)
,
joined as (
	select 
		l.max_depth_m,
		geo.*,
		|/ ((l.latitude::numeric - geo.latitude)^2 + (l.longitude::numeric - geo.longitude)^2 ) as distance
	from lake l 
	right join geo 
	on l.lake_name = geo.lake_name
)
,
ranked as (
	select *,
		distance < 1 as is_close,
		row_number() OVER(partition by id order by distance desc) as rnk
	from joined
)

select 
--	count(1)
	boundary,
	bounding_box,
	geometry,
	id,
	longitude::varchar,
	latitude::varchar,
	gnis_name,
	case when is_close THEN max_depth_m	end as max_depth_m,
	areasqkm,
	elevation,
	bounds[1] as min_longitude,
	bounds[3] as max_longitude,
	bounds[2] as min_latitude,
	bounds[4] as max_latitude
from ranked
where rnk = 1


"""