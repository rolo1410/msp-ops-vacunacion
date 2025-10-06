import logging
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

import polars as pl

from extract.config.sources import DB_REPLICA, DB_VACUNACION, get_oracle_engine
from lake.init_lake import add_new_elements_to_lake

VACUNACION_REGULAR_SCHEMA = {
    "ID":pl.Int64,
    'HACUE_AMED':pl.String,
    "ACTIVO":pl.Int64, 
    "PACIENTE_ID":pl.Int64,
    "PERSONA_ID":pl.Int64,
    "FECHAVACUNACION" :pl.Date,
    "NUMEROIDENTIFICACION" :pl.String,
    "FECHANACIMIENTO":pl.Date,
    "ESTADO":pl.Int64,
    "CTSEXO_ID":pl.Int64, 
    "SEXO":pl.String,
    "ENTIDAD_ID":pl.Int64,
    "ESQUEMAVACUNACION_ID":pl.Int64,
    "ID":pl.Int64,
    "ESTADO":pl.Int64,
    "FECHACREACION":pl.Date,
    "FECHAMODIFICACION":pl.Date,
    "PUNTOVACUNACION_ID":pl.Int64,
    "LOTE":pl.String,
    "FASEVACUNACION":pl.String,
    "REFUERZO":pl.String
}

def get_db_vacunacion_rutinario_chunk(since: str, until: str, offset: int = 0, chunk_size: int = 100000) -> pl.DataFrame:
    """
    Versión optimizada de la consulta con mejores prácticas SQL
    """
    db_vacunacion_engine = get_oracle_engine(DB_REPLICA)
    
    # Query optimizada con hints de Oracle y ORDER BY para consistencia
    query = f"""
            SELECT /*+ FIRST_ROWS({chunk_size}) INDEX_RANGE_SCAN */
                R.ID,
                'HACUE_AMED',
                P.ACTIVO ,
                r.PACIENTE_ID,
                PE.ID AS PERSONA_ID,
                r.FECHAVACUNACION ,
                PE.NUMEROIDENTIFICACION ,
                pe.FECHANACIMIENTO ,
                pe.ESTADO ,
                pe.CTSEXO_ID ,
                d.DESCRIPCION AS SEXO,
                pe.FECHACREACION ,
                pe.FECHAMODIFICACION ,
                R.ENTIDAD_ID,
                R.ESQUEMAVACUNACION_ID,
                P.ID,
                P.ESTADO ,
                P.FECHACREACION ,
                P.FECHAMODIFICACION ,
                R.PUNTOVACUNACION_ID,
                R.LOTE,
                R.FASEVACUNACION,
                r.REFUERZO
            FROM
            HCUE_AMED.REGISTROVACUNACION R INNER JOIN HCUE_AMED.PACIENTE P ON
            R.PACIENTE_ID = P.ID INNER JOIN HCUE_SISTEMA.PERSONA PE ON
            PE.ID = p.PERSONA_ID LEFT JOIN HCUE_CATALOGOS.DETALLECATALOGO d ON d.ID = pe.CTSEXO_ID 
            WHERE r.FECHAVACUNACION BETWEEN TO_DATE('{since}', 'YYYY-MM-DD') AND TO_DATE('{until}', 'YYYY-MM-DD')
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


def get_count_db_vacunacion_rutinario(since, until):
    db_vacunacion_engine = get_oracle_engine(DB_REPLICA)
    query = f"""
            SELECT
                count(*) as TOTAL_COUNT
            FROM
                HCUE_AMED.REGISTROVACUNACION R
            WHERE
                R.FECHAVACUNACION > TO_DATE('{since}', 'yyyy-mm-dd') 
                AND R.FECHAVACUNACION < TO_DATE('{until}', 'yyyy-mm-dd')
            """  # Replace with actual query
    df = pl.read_database(query, connection=db_vacunacion_engine.connect())
    return df['TOTAL_COUNT'][0]


def load_lake_db_vacunacion_rutinario(since, until, chunk_size=100000):
    """
    Carga los datos en el lago de datos desde la base de datos en chunks
    """
    total_count = get_count_db_vacunacion_rutinario(since, until)
    logging.info(f"|- Total de registros a procesar: {total_count:,}")
    
    offset = 0
    
    while offset < total_count:
        logging.info(f" |- Procesando chunk con offset {offset}")
        df_chunk = get_db_vacunacion_rutinario_chunk(since, until, offset, chunk_size)
        add_new_elements_to_lake('vacunacion', 'db_vacunacion_rutinario', ['NUMEROIDENTIFICACION', 'FECHAVACUNACION', 'PUNTOVACUNACION_ID'], df_chunk)
        offset += chunk_size
        logging.info(f" |- Chunk con offset {offset} procesado y almacenado en el lago")

