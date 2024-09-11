import sqlmodel
import json
import os
import boto3
from sqlmodel import create_engine
import sqlalchemy

secret_arn = os.environ.get("DB_CREDS_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:117819748843:secret:main-rds-db-creds")

secret = json.loads(
        boto3.client("secretsmanager", 'us-east-1')
        .get_secret_value(SecretId=secret_arn)
        ["SecretString"]
)

db_username = secret["username"]

db_password = secret["password"]

db_endpoint = secret["host"]

engine = sqlmodel.create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{db_endpoint}', echo=True) #/lake_freeze

sqlalchemy_engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{db_endpoint}', echo=True)

