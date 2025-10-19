import logging
import re

import duckdb
import pandas as pd
import polars as pl


def generate_lake_schema():
    # Implement the logic to generate the lake schema
    con = duckdb.connect('./resources/data_lake/vacunacion.duckdb')
    con.execute("""
        CREATE TABLE IF NOT EXISTS lake_schema (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            value DOUBLE
        )
    """)
    con.close()


def generate_bi_schema():
    # Implement the logic to generate the BI schema
    con = duckdb.connect('./resources/data_lake/vacunacion_schema.duckdb')
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_persona (
            id INTEGER PRIMARY KEY,
            nombres VARCHAR,
            apellidos VARCHAR,
            fecha_nacimiento DATE,
            identificacion VARCHAR,
            tipo_identificacion VARCHAR,
            nacionalidad VARCHAR,
            pueblo VARCHAR,
            etnia VARCHAR,
            sexo VARCHAR           
        );
        CREATE TABLE IF NOT EXISTS dim_vacuna (
            id INTEGER PRIMARY KEY,
            nombre VARCHAR,
            lote VARCHAR
        );
        CREATE TABLE IF NOT EXISTS dim_tiempo (
            id INTEGER PRIMARY KEY,
            fecha DATE,
            dia INTEGER,
            mes INTEGER,
            anio INTEGER,
            trimestre INTEGER,
            semestre INTEGER,
            dia_semana VARCHAR,
            es_fin_de_semana BOOLEAN
        );
        CREATE TABLE IF NOT EXISTS dim_establecimiento(
            id INTEGER PRIMARY KEY,
            pais VARCHAR,
            uni_codigo VARCHAR,
            uni_nombre VARCHAR,
            uni_tipo VARCHAR,
            correo VARCHAR
        );      
        CREATE TABLE IF NOT EXISTS dim_dpa_administrativo(
            id INTEGER PRIMARY KEY,
            zona VARCHAR,
            codigo_zona VARCHAR,
            circuito VARCHAR,
            codigo_circuito VARCHAR,
            distrito VARCHAR,
            codigo_distrito VARCHAR
        );
        CREATE TABLE IF NOT EXISTS dim_dpa_geografico(
            id INTEGER PRIMARY KEY,
            provincia VARCHAR,
            codigo_provincia VARCHAR,
            canton VARCHAR,
            codigo_canton VARCHAR,
            parroquia VARCHAR,
            codigo_parroquia VARCHAR
        );
        CREATE TABLE IF NOT EXISTS dim_profesional(
            id INTEGER PRIMARY KEY,
            nombres VARCHAR,
            identificacion VARCHAR
        );
        CREATE TABLE IF NOT EXISTS fact_vacunacion (
            id INTEGER PRIMARY KEY,
            persona_id INTEGER,
            vacuna_id INTEGER,
            profesional_id INTEGER,
            establecimiento_id INTEGER,
            dpa_administrativo_id INTEGER,
            dpa_geografico_id INTEGER,
            tiempo_id INTEGER,
            fecha_vacunacion DATE,
            centro_vacunacion VARCHAR,
            FOREIGN KEY (persona_id) REFERENCES dim_persona(id),
            FOREIGN KEY (dpa_administrativo_id) REFERENCES dim_dpa_administrativo(id),
            FOREIGN KEY (dpa_geografico_id) REFERENCES dim_dpa_geografico(id),
            FOREIGN KEY (tiempo_id) REFERENCES dim_tiempo(id),
            FOREIGN KEY (vacuna_id) REFERENCES dim_vacuna(id),
            FOREIGN KEY (profesional_id) REFERENCES dim_profesional(id),
            FOREIGN KEY (establecimiento_id) REFERENCES dim_establecimiento(id)
        );
    """)
    con.close()
    
def add_new_elements_to_lake(db: str,
                             table: str,
                             keys_columns: list[str],
                             df: pd.DataFrame | pl.DataFrame):  # Removed type hint to accept both pandas and polars
    logging.info(f"|-Adding new elements to lake aqui: {db}.{table}")
    # Implement the logic to add new elements to the lake
    try:
    # aqui hace el llamado al df
        columnas = ','.join(df.columns)
        con = duckdb.connect(f'./resources/data_lake/{db}.duckdb')
        query= f"""CREATE TABLE IF NOT EXISTS {db}.main.{table} AS SELECT * FROM df;"""
        query2=f"""CREATE TABLE IF NOT EXISTS {db}.main.tmp_{table} AS SELECT * FROM df;"""
        # Insert sólo las columnas compatibles entre la tabla destino y la tabla temporal
        # Verificar si la tabla destino existe
        table_exists = con.execute(
            f"SELECT 1 FROM information_schema.tables WHERE table_schema='main' AND table_name='{table}' LIMIT 1"
        ).fetchall()
        if table_exists:
            # Obtener columnas existentes en la tabla destino
            col_info = con.execute(f"PRAGMA table_info('{table}')").fetchall()
            existing_cols = [r[1] for r in col_info]  # PRAGMA table_info -> (cid, name, type, ...)
            df_cols = list(df.columns)
            compatible_cols = [c for c in df_cols if c in existing_cols]

            if not compatible_cols:
                logging.warning(f"No hay columnas compatibles para insertar en {db}.main.{table}")
                query3 = "SELECT 1;"
            else:
            # Filtrar las claves que también sean compatibles
                keys_filtered = [c for c in keys_columns if c in compatible_cols]
            if not keys_filtered:
                logging.warning(f"No hay columnas clave compatibles para evitar duplicados en {db}.main.{table}")
                query3 = "SELECT 1;"
            else:
                cols_list = ','.join([f'"{c}"' for c in compatible_cols])
                where_clause = ' AND '.join([f't."{c}" = tmp."{c}"' for c in keys_filtered])
                query3 = (
                f"""INSERT INTO {db}.main.{table} ({cols_list})
                    SELECT {cols_list} FROM {db}.main.tmp_{table} AS tmp
                    WHERE NOT EXISTS (
                    SELECT 1 FROM {db}.main.{table} AS t WHERE {where_clause}
                    );"""
                )
        else:
            # Si la tabla no existe, la query de CREATE TABLE AS SELECT rellenará la tabla; no hace falta insertar
            query3 = "SELECT 1;"
        query4= f"""DROP TABLE {db}.main.tmp_{table};"""
        con.execute(query)
        con.execute(query2)
        con.execute(query3)
        con.execute(query4)
        con.close()
    except Exception as e:
        logging.error(f"Error adding new elements to lake {db}.{table}: {e}")   
        con.close()
        
def eliminar_tabla_tmp(db: str,
                             table: str):
    logging.info(f"|-Eliminando tabla temporal del lago: {db}.tmp_{table}")
    # Implement the logic to add new elements to the lake
    try:
        con = duckdb.connect(f'./resources/data_lake/{db}.duckdb')
        query= f"""DROP TABLE IF EXISTS {db}.main.tmp_{table};"""
        con.execute(query)
        con.close()
    except Exception as e:
        logging.error(f"Error eliminando tabla temporal del lago {db}.tmp_{table}: {e}")   
        con.close()