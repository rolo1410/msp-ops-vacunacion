import logging
import time
from venv import logger

import oracledb
import pandas as pd
import polars as pl

from extract.config.sources import DB_VACUNACION, get_oracle_engine
from lake.init_lake import add_new_elements_to_lake

VACUNACION_SCHEMA = {
    "ID_VAC_DEPU": pl.String,
    "FECHA_APLICACION": pl.Date,
    "PUNTO_VACUNACION": pl.String,
    "UNICODIGO": pl.String,
    "TIPO_IDEN": pl.String,
    "NUM_IDEN": pl.String,
    "APELLIDOS": pl.String,
    "NOMBRES": pl.String,
    "NOMBRES_COMPLETOS": pl.String,
    "SEXO": pl.String,
    "FECHA_NACIMIENTO": pl.Date,
    "NACIONALIDAD": pl.String,
    "ETNIA": pl.String,
    "POBLA_VACUNA": pl.String,
    "GRUPO_RIESGO": pl.String,
    "NOMBRE_VACUNA": pl.String,
    "LOTE_VACUNA": pl.String,
    "DOSIS_APLICADA": pl.String,
    "PROFESIONAL_APLICA": pl.String,
    "IDEN_PROFESIONAL_APLICA": pl.String,
    "FASE_VACUNA": pl.String,
    "FASE_VACUNA_DEPURADA": pl.String,
    "GRUPO_RIESGO_DEPURADA": pl.String,
    "SISTEMA": pl.String,
    "REGISTRO_CIVIL": pl.String,
    "ID_VAC_CONS": pl.String,
}

def get_db_vacunacion_covid_chunk(since: str, until: str, offset: int = 0, chunk_size: int = 100000) -> pl.DataFrame:
    """
    Versión optimizada de la consulta con mejores prácticas SQL
    """
    db_vacunacion_engine = get_oracle_engine(DB_VACUNACION)
    
    # Query optimizada con hints de Oracle y ORDER BY para consistencia
    query = f"""
            SELECT /*+ FIRST_ROWS({chunk_size}) INDEX_RANGE_SCAN */
                ID_VAC_DEPU,
                FECHA_APLICACION,
                PUNTO_VACUNACION,
                UNICODIGO,
                TIPO_IDEN,
                NUM_IDEN,
                APELLIDOS,
                NOMBRES,
                NOMBRES_COMPLETOS,
                SEXO,
                FECHA_NACIMIENTO,
                NACIONALIDAD,
                ETNIA,
                POBLA_VACUNA,
                GRUPO_RIESGO,
                NOMBRE_VACUNA,
                LOTE_VACUNA,
                DOSIS_APLICADA,
                PROFESIONAL_APLICA,
                IDEN_PROFESIONAL_APLICA,
                FASE_VACUNA,
                FASE_VACUNA_DEPURADA,
                GRUPO_RIESGO_DEPURADA,
                SISTEMA,
                REGISTRO_CIVIL,
                ID_VAC_CONS
            FROM HCUE_VACUNACION_DEPURADA.DB_VACUNACION_CONSOLIDADA_DEPURADA_COVID
            WHERE FECHA_APLICACION BETWEEN TO_DATE('{since}', 'YYYY-MM-DD') AND TO_DATE('{until}', 'YYYY-MM-DD')
            OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY
            """
    try:
        start_time = time.time()
        df = pl.read_database(query, connection=db_vacunacion_engine.connect(), infer_schema_length=None)
        end_time = time.time()
        logging.debug(f" |- Chunk consultado en {end_time - start_time:.2f} segundos")
        return df
    except Exception as e:
        logging.error(f"Error en consulta chunk offset {offset}: {e}")
        return pl.DataFrame()

def get_count_db_vacunacion(since, until):
    db_vacunacion_engine = get_oracle_engine(DB_VACUNACION)
    query = f"""
            SELECT 
                COUNT(*) AS total_count
            FROM HCUE_VACUNACION_DEPURADA.DB_VACUNACION_CONSOLIDADA_DEPURADA_COVID
            WHERE 
                FECHA_APLICACION BETWEEN TO_DATE('{since}', 'YYYY-MM-DD') 
                AND TO_DATE('{until}', 'YYYY-MM-DD')
            """  # Replace with actual query
    df = pl.read_database(query, connection=db_vacunacion_engine.connect())
    return df['TOTAL_COUNT'][0]

def load_lake_db_vacunacion_covid(since: str, until: str, chunk_size: int = 1000000):
    """
    Carga datos de vacunación COVID en paralelo con persistencia directa en DuckDB
    """
    ## TODO: borrar conexión
    connection = oracledb.connect(
        user="USR.ROLANDOCASIGNA",
        password="Salud.2025",
        host="scan19c-mspvacuna-prod.msp.gob.ec",
        port=1521,
        service_name="DB_VACUNACION"
    )
    
    query = f"""
            SELECT 
                *
            FROM HCUE_VACUNACION_DEPURADA.DB_VACUNACION_CONSOLIDADA_DEPURADA_COVID
            WHERE 
                FECHA_APLICACION BETWEEN TO_DATE('{since}', 'YYYY-MM-DD') 
                AND TO_DATE('{until}', 'YYYY-MM-DD')
            """
        
    chunk_size = 10000000 # Procesar en chunks de 50k registros
    count=0
    for chunk_df in pd.read_sql(query, connection, chunksize=chunk_size):
        chunk_df.columns = [col.lower() for col in chunk_df.columns]
        add_new_elements_to_lake('vacunacion', 'lk_vacunacion_covid', ['num_iden','fecha_aplicacion', 'tipo_iden', 'nombre_vacuna', 'lote_vacuna', 'fase_vacuna_depurada'], chunk_df)
        logger.info(f"Procesado chunk con {len(chunk_df)} registros")
        count += 1
    logger.info(f"Archivos data lake creados exitosamente")

    
VACUNACION_COLUMNS = ['id_vac_depu', 'fecha_aplicacion', 'punto_vacunacion', 'unicodigo', 'tipo_iden', 'num_iden', 'apellidos', 'nombres', 'nombres_completos', 'sexo', 'fecha_nacimiento', 'nacionalidad', 'etnia', 'pobla_vacuna', 'grupo_riesgo', 'nombre_vacuna', 'lote_vacuna', 'dosis_aplicada', 'profesional_aplica', 'iden_profesional_aplica', 'fase_vacuna', 'fase_vacuna_depurada', 'grupo_riesgo_depurada', 'sistema', 'registro_civil', 'id_vac_cons']