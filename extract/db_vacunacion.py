import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
import time

import polars as pl

from extract.config.sources import DB_VACUNACION, get_oracle_engine

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

def get_db_vacunacion_optimized(since: str, until: str, offset: int = 0, chunk_size: int = 100000) -> pl.DataFrame:
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
            ORDER BY ID_VAC_DEPU
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

# Mantener función original para compatibilidad
def get_db_vacunacion(since, until, offset=0, chunk_size=100000) -> pl.DataFrame:
    return get_db_vacunacion_optimized(since, until, offset, chunk_size)


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


def _fetch_chunk_worker(args) -> Optional[pl.DataFrame]:
    """Worker function para procesamiento paralelo de chunks"""
    since, until, offset, chunk_size = args
    try:
        chunk = get_db_vacunacion_optimized(since, until, offset, chunk_size)
        if not chunk.is_empty():
            chunk = chunk.cast(VACUNACION_SCHEMA, strict=False)
            return chunk
        return None
    except Exception as e:
        logging.error(f"Error procesando chunk offset {offset}: {e}")
        return None

def get_db_vacunaciones_parallel(since: str, until: str, chunk_size: int = 500000, max_workers: int = 4) -> pl.DataFrame:
    """
    Versión paralela optimizada para obtener datos de vacunación
    """
    logging.info(f"|- Fetching vacunas (paralelo con {max_workers} workers)")
    start_total = time.time()
    
    # Obtener total de registros
    total = get_count_db_vacunacion(since, until)
    logging.info(f" |- Total de registros: {total:,}")
    
    if total == 0:
        return pl.DataFrame()
    
    # Preparar argumentos para workers
    chunk_args = []
    for offset in range(0, total, chunk_size):
        chunk_args.append((since, until, offset, chunk_size))
    
    logging.info(f" |- Procesando {len(chunk_args)} chunks con {max_workers} workers")
    
    all_data = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Enviar todos los trabajos
        future_to_offset = {
            executor.submit(_fetch_chunk_worker, args): args[2] 
            for args in chunk_args
        }
        
        # Recoger resultados conforme se completan
        for future in as_completed(future_to_offset):
            offset = future_to_offset[future]
            try:
                chunk = future.result()
                if chunk is not None:
                    all_data.append(chunk)
                    logging.info(f" |- Completado chunk offset {offset} ({len(all_data)}/{len(chunk_args)})")
            except Exception as e:
                logging.error(f"Error en chunk offset {offset}: {e}")
    
    # Concatenar todos los chunks
    if all_data:
        logging.info(f" |- Concatenando {len(all_data)} chunks...")
        result = pl.concat(all_data, rechunk=True)
        end_total = time.time()
        logging.info(f" |- Procesamiento completado en {end_total - start_total:.2f} segundos")
        logging.info(f" |- DataFrame final: {result.shape[0]:,} filas, {result.shape[1]} columnas")
        return result
    else:
        logging.warning(" |- No se obtuvieron datos")
        return pl.DataFrame()

def get_db_vacunaciones_cached(since: str, until: str, chunk_size: int = 500000, 
                              cache_dir: str = "./resources/cache") -> pl.DataFrame:
    """
    Versión con cache para evitar consultas repetidas
    """
    # Crear directorio de cache si no existe
    os.makedirs(cache_dir, exist_ok=True)
    
    # Nombre del archivo de cache basado en parámetros
    cache_filename = f"vacunaciones_{since}_{until}_{chunk_size}.parquet"
    cache_path = os.path.join(cache_dir, cache_filename)
    
    # Verificar si existe cache válido
    if os.path.exists(cache_path):
        try:
            logging.info(f"|- Cargando datos desde cache: {cache_filename}")
            df = pl.read_parquet(cache_path)
            logging.info(f" |- Cache cargado: {df.shape[0]:,} filas, {df.shape[1]} columnas")
            return df
        except Exception as e:
            logging.warning(f" |- Error cargando cache: {e}, consultando base de datos")
    
    # Si no hay cache válido, consultar base de datos
    logging.info("|- No hay cache válido, consultando base de datos")
    df = get_db_vacunaciones_parallel(since, until, chunk_size)
    
    # Guardar en cache si se obtuvieron datos
    if not df.is_empty():
        try:
            logging.info(f"|- Guardando datos en cache: {cache_filename}")
            df.write_parquet(cache_path)
            logging.info(" |- Cache guardado exitosamente")
        except Exception as e:
            logging.warning(f" |- Error guardando cache: {e}")
    
    return df

# Mantener función original para compatibilidad, pero usar la versión optimizada
def get_db_vacunaciones(since, until, chunk_size=500000) -> pl.DataFrame:
    """
    Función principal optimizada - usa paralelización por defecto
    """
    # Determinar número óptimo de workers basado en CPU
    max_workers = min(4, os.cpu_count() or 1)
    
    # Usar versión con cache si está disponible
    try:
        return get_db_vacunaciones_cached(since, until, chunk_size)
    except Exception as e:
        logging.warning(f"Error con cache, usando versión paralela: {e}")
        return get_db_vacunaciones_parallel(since, until, chunk_size, max_workers)
