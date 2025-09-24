import logging

import duckdb
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


def generare_bi_echema():
    # Implement the logic to generate the BI schema
    con = duckdb.connect('./resources/data_lake/vacunacion.duckdb')
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
    
def add_new_elements_to_lake(db:str,
                              table:str,
                              keys_columns:list[str],
                              df:pl.DataFrame):
    logging.info(f"|-Adding new elements to lake: {db}.{table}")
    # Implement the logic to add new elements to the lake
    con = duckdb.connect(f'./resources/data_lake/{db}.duckdb')
    
    # 
    one_query = f"""CREATE TABLE IF NOT EXISTS {db}.main.{table} AS SELECT * FROM df;
                    CREATE TABLE IF NOT EXISTS {db}.main.tmp_{table} AS SELECT * FROM df;
                    INSERT INTO {db}.main.{table} SELECT * FROM {db}.main.tmp_{table} WHERE NOT EXISTS (SELECT 1 FROM {db}.main.{table} WHERE {' AND '.join([f'{table}.{col} = tmp_{table}.{col}' for col in keys_columns])} );
                    DROP TABLE {db}.main.tmp_{table};"""
    #
    con.execute(one_query)
    con.close()
    