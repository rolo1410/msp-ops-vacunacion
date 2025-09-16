import os

import dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

dotenv.load_dotenv()

DB_VACUNACION = {
    "host": os.getenv("CNN_ORACLE_DB_VACUNACION_HOST", "localhost"),
    "port": int(os.getenv("CNN_ORACLE_DB_VACUNACION_PORT", 1521)),
    "user": os.getenv("CNN_ORACLE_DB_VACUNACION_USER", "user"),
    "password": os.getenv("CNN_ORACLE_DB_VACUNACION_PASSWORD", "password"),
    "service_name": os.getenv("CNN_ORACLE_DB_VACUNACION_SID", "orcl")
}

DB_MIP = {
    "host": os.getenv("CNN_ORACLE_DB_MPI_HOST", "localhost"),
    "port": int(os.getenv("CNN_ORACLE_DB_MPI_PORT", 1521)),
    "user": os.getenv("CNN_ORACLE_DB_MPI_USER", "user"),
    "password": os.getenv("CNN_ORACLE_DB_MPI_PASSWORD", "password"),
    "service_name": os.getenv("CNN_ORACLE_DB_MPI_SID", "orcl")
}

DB_GEOSALUD = {
    "host": os.getenv("CNN_ORACLE_DB_GEOSALUD_HOST", "localhost"),
    "port": int(os.getenv("CNN_ORACLE_DB_GEOSALUD_PORT", 1521)),
    "user": os.getenv("CNN_ORACLE_DB_GEOSALUD_USER", "user"),
    "password": os.getenv("CNN_ORACLE_DB_GEOSALUD_PASSWORD", "password"),
    "service_name": os.getenv("CNN_ORACLE_DB_GEOSALUD_DB_NAME", "geoserver")
}

DB_REPLICA = {
    "host": os.getenv("CNN_ORACLE_DB_REPLICACION_HOST", "localhost"),
    "port": int(os.getenv("CNN_ORACLE_DB_REPLICACION_PORT", 1521)),
    "user": os.getenv("CNN_ORACLE_DB_REPLICACION_USER", "user"),
    "password": os.getenv("CNN_ORACLE_DB_REPLICACION_PASSWORD", "password"),
    "service_name": os.getenv("CNN_ORACLE_DB_REPLICACION_DBNAME", "replicacion")
}


def get_oracle_engine(options: dict) -> Engine:
    user = options.get("user", "user")
    password = options.get("password", "password")
    host = options.get("host", "localhost")
    port = options.get("port", 1521)
    service_name = options.get("service_name", "orclpdb1")
    connection_string = f'oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service_name}'
    print(connection_string)
    engine: Engine = create_engine(connection_string, pool_pre_ping=True)
    return engine


def postgres_get_engine(options: dict) -> Engine:
    user = options.get("user", "user")
    password = options.get("password", "password")
    host = options.get("host", "localhost")
    port = options.get("port", 5432)
    dbname = options.get("dbname", "dbname")
    connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
    engine: Engine = create_engine(connection_string, pool_pre_ping=True)
    return engine
