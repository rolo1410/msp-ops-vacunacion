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
            nombres VARCHAR),
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
            lote VARCHAR,
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
        CREATE TABLE IF NOT EXISTS fact_vacunacion (
            id INTEGER PRIMARY KEY,
            persona_id INTEGER,
            vacuna_id INTEGER,
            fecha_vacunacion DATE,
            centro_vacunacion VARCHAR,
            FOREIGN KEY (persona_id) REFERENCES dim_persona(id),
            FOREIGN KEY (vacuna_id) REFERENCES dim_vacuna(id)
        );
    """)
    con.close()
    
def add_new_elements_to_lake(db:str,
                              table:str,
                              keys_columns:list[str],
                              df:pl.DataFrame):
    
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
    