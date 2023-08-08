

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

from data_models import *

from pyspark.sql import SparkSession

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

# s3 = boto3.client('s3')

s3_bucket, s3_prefix = 'prd-tnm', 'StagedProducts/Hydrography/NHD/State/Shape/'
local_data_dir = 'raw_data'
# ls_resp = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

states = ['Minnesota'] #us.states.STATES_AND_TERRITORIES


def load_geometry_data():
    for state in states:
        filename = f"NHD_H_{state}_State_Shape.zip"
        s3_key = s3_prefix + filename

        local_filename = os.path.join(local_data_dir, filename) 

        # if not os.path.exists(local_filename):
        #     s3.download_file(s3_bucket, s3_key, local_filename)

        with open(local_filename, 'rb') as f:
            zip_bytes = f.read()

        waterbody_dbf_filename_in_zip = "Shape/NHDWaterbody.dbf"
        local_waterbody_dbf_filename = os.path.join("raw_data", f"{state}_NHDWaterbody.dbf")

        waterbody_shp_filename_in_zip = "Shape/NHDWaterbody.shp"
        local_waterbody_shp_filename = os.path.join("raw_data", f"{state}_NHDWaterbody.shp")

        waterbody_shx_filename_in_zip = "Shape/NHDWaterbody.shx"
        local_waterbody_shx_filename = os.path.join("raw_data", f"{state}_NHDWaterbody.shx")

        
        with zipfile.ZipFile(io.BytesIO(zip_bytes), 'r') as z: # 
            # with z.open(waterbody_dbf_filename_in_zip, 'r') as zip_f:
            #     with open(local_waterbody_dbf_filename, 'wb') as f:
            #         f.write(zip_f.read())

            #     waterbodies: pd.DataFrame = Dbf5(local_waterbody_dbf_filename).to_dataframe()
            #     lakes = waterbodies.dropna(subset=['gnis_name']) 
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

        lakes = shp.dropna(subset=['gnis_name']) # Filter out water bodies with no name

        def get_coords(polygon: Polygon):
            # if len(polygon.__geo_interface__['coordinates']) != 1:
            #     raise Exception(f"Bad geometry {polygon.__geo_interface__['coordinates']}")
            
            poly = polygon.__geo_interface__['coordinates']
            
            # new_poly = []
            # for point in poly:
            #     x, y, z = point
            #     assert z == 0.0
            #     new_poly.append([x, y])
            
            return json.dumps(poly)

        lakes['geometry'] = lakes['geometry'].apply(get_coords)

        print(lakes )

        # (
        #     spark.createDataFrame(lakes)
        #     .write
        #     .option("dbtable", "lake_geometry")
        #     .options(**get_jdbc_options())
        #     .format("jdbc")
        #     .mode("append")
        #     .save()
        # )

        from sqlalchemy.dialects import postgresql

        lakes.to_sql(name='lake_geometry', con=sqlalchemy_engine, if_exists='append', index=False
                    )

        # print(shp)



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