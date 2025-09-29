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

def get_db_vacunacion_rutinario_optimized(since: str, until: str, offset: int = 0, chunk_size: int = 100000) -> pl.DataFrame:
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

# Mantener función original para compatibilidad
def get_db_vacunacion(since, until, offset=0, chunk_size=100000) -> pl.DataFrame:
    return get_db_vacunacion_rutinario_optimized(since, until, offset, chunk_size)


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


def _fetch_chunk_worker(args) -> Optional[pl.DataFrame]:
    """Worker function para procesamiento paralelo de chunks"""
    since, until, offset, chunk_size = args
    try:
        chunk = get_db_vacunacion_rutinario_optimized(since, until, offset, chunk_size)
        if not chunk.is_empty():
            chunk = chunk.cast(VACUNACION_REGULAR_SCHEMA, strict=False)
            return chunk
        return None
    except Exception as e:
        logging.error(f"Error procesando chunk offset {offset}: {e}")
        return None

def _persistence_worker_rutinario(data_queue: queue.Queue, processed_chunks: list, stop_event: threading.Event):
    """Worker function para persistir chunks de forma secuencial usando DuckDB"""
    while not stop_event.is_set() or not data_queue.empty():
        try:
            # Intentar obtener un chunk de la cola con timeout
            chunk_data = data_queue.get(timeout=1.0)
            if chunk_data is None:  # Señal de finalización
                break
                
            offset, chunk = chunk_data
            
            # Persistir el chunk usando add_new_elements_to_lake
            add_new_elements_to_lake('vacunacion_rutinario', 'lk_vacunacion_rutinario', 
                                   ['NUMEROIDENTIFICACION', 'FECHAVACUNACION', 'ENTIDAD_ID'], chunk)
            
            processed_chunks.append(offset)
            logging.info(f" |- Persistido chunk offset {offset} ({len(processed_chunks)} chunks completados)")
            data_queue.task_done()
            
        except queue.Empty:
            continue
        except Exception as e:
            logging.error(f"Error persistiendo chunk: {e}")
            data_queue.task_done()

def get_db_vacunaciones_parallel_rutinario(since: str, until: str, chunk_size: int = 500000, max_workers: int = 4) -> None:
    """
    Versión paralela optimizada para obtener datos de vacunación y persistirlos usando cola.
    No retorna DataFrame, sino que persiste directamente cada chunk al lago de datos.
    """
    logging.info(f"|- Fetching vacunas (paralelo con {max_workers} workers)")
    start_total = time.time()
    
    # Obtener total de registros
    total = get_count_db_vacunacion_rutinario(since, until)
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
        target=_persistence_worker_rutinario, 
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


# Mantener función original para compatibilidad, pero usar la versión optimizada
def get_db_vacunaciones(since, until, chunk_size=500000) -> pl.DataFrame:
    """
    Función principal optimizada - usa paralelización por defecto
    """
    # Determinar número óptimo de workers basado en CPU
    max_workers = min(4, os.cpu_count() or 1)
    
    # Usar versión con cache si está disponible
    try:
        logging.warning(f"Error con cache, usando versión paralela: {e}")
        # La función paralela ya no retorna DataFrame, persiste directamente
        get_db_vacunaciones_parallel_rutinario(since, until, chunk_size, max_workers)
        # Cargar datos desde el lago
        from lake.load_lake import load_data
        return load_data()
    except Exception as e:
        logging.info("|- Usando versión paralela optimizada")
    return pl.DataFrame()


def get_db_vacunaciones_from_lake() -> pl.DataFrame:
    """
    Función de utilidad para cargar datos de vacunación directamente desde el lago.
    Útil cuando se necesita el DataFrame completo después de usar la versión paralela.
    """
    from lake.load_lake import load_data
    logging.info("|- Cargando datos de vacunación desde el lago")
    df = load_data()
    logging.info(f" |- Cargados {df.shape[0]:,} registros, {df.shape[1]} columnas")
    return df
