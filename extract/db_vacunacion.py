import logging
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

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

def _persistence_worker(data_queue: queue.Queue, processed_chunks: list, stop_event: threading.Event):
    """Worker function para persistir chunks de forma secuencial usando DuckDB"""
    while not stop_event.is_set() or not data_queue.empty():
        try:
            # Intentar obtener un chunk de la cola con timeout
            chunk_data = data_queue.get(timeout=1.0)
            if chunk_data is None:  # Señal de finalización
                break
                
            offset, chunk = chunk_data
            
            # Limpiar la columna NUM_IDEN antes de persistir
            if 'NUM_IDEN' in chunk.columns:
                chunk = chunk.with_columns(
                    chunk['NUM_IDEN'].str.replace_all("'", "").cast(str)
                )
            
            # Persistir el chunk usando add_new_elements_to_lake
            add_new_elements_to_lake('vacunacion', 'lk_vacunacion', 
                                   ['NUM_IDEN', 'FECHA_APLICACION', 'UNICODIGO'], chunk)
            
            processed_chunks.append(offset)
            logging.info(f" |- Persistido chunk offset {offset} ({len(processed_chunks)} chunks completados)")
            data_queue.task_done()
            
        except queue.Empty:
            continue
        except Exception as e:
            logging.error(f"Error persistiendo chunk: {e}")
            data_queue.task_done()

def get_db_vacunaciones_parallel(since: str, until: str, chunk_size: int = 500000, max_workers: int = 4) -> None:
    """
    Versión paralela optimizada para obtener datos de vacunación y persistirlos usando cola.
    No retorna DataFrame, sino que persiste directamente cada chunk al lago de datos.
    """
    logging.info(f"|- Fetching vacunas (paralelo con {max_workers} workers)")
    start_total = time.time()
    
    # Obtener total de registros
    total = get_count_db_vacunacion(since, until)
    logging.info(f" |- Total de registros: {total:,}")
    
    if total == 0:
        logging.warning(" |- No hay registros para procesar")
        return
    
    # Preparar argumentos para workers
    chunk_args = []
    for offset in range(0, total, chunk_size):
        chunk_args.append((since, until, offset, chunk_size))
    
    logging.info(f" |- Procesando {len(chunk_args)} chunks con {max_workers} workers")
    
    # Crear cola para comunicación entre threads
    data_queue = queue.Queue(maxsize=5)  # Limitar tamaño para controlar memoria
    processed_chunks = []
    stop_event = threading.Event()
    
    # Iniciar worker de persistencia en thread separado
    persistence_thread = threading.Thread(
        target=_persistence_worker, 
        args=(data_queue, processed_chunks, stop_event)
    )
    persistence_thread.start()
    
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Enviar todos los trabajos de descarga
            future_to_offset = {
                executor.submit(_fetch_chunk_worker, args): args[2] 
                for args in chunk_args
            }
            
            # Recoger resultados conforme se completan y enviarlos a la cola
            for future in as_completed(future_to_offset):
                offset = future_to_offset[future]
                try:
                    chunk = future.result()
                    if chunk is not None:
                        # Poner el chunk en la cola para persistencia
                        data_queue.put((offset, chunk))
                        logging.info(f" |- Descargado chunk offset {offset}, enviado a cola de persistencia")
                    else:
                        logging.warning(f" |- Chunk offset {offset} está vacío")
                except Exception as e:
                    logging.error(f"Error en chunk offset {offset}: {e}")
    
    finally:
        # Señalar fin de procesamiento
        data_queue.put(None)  # Señal de parada
        stop_event.set()
        
        # Esperar a que termine el thread de persistencia
        persistence_thread.join()
        
        end_total = time.time()
        logging.info(f" |- Procesamiento completado en {end_total - start_total:.2f} segundos")
        logging.info(f" |- Total de chunks persistidos: {len(processed_chunks)}/{len(chunk_args)}")
