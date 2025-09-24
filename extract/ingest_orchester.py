from extract.db_vacunacion import get_db_vacunaciones, get_db_vacunaciones_cached, get_db_vacunaciones_parallel
from extract.geo_salud import get_geo_salud_data
from extract.mpi import get_mpi_data
from lake.init_lake import add_new_elements_to_lake
from lake.load_lake import load_data
import logging


def ingest_orchester(since, until, chunk_size=500000, max_workers=4, use_cache=True):
    """
    Orquestador de ingesta optimizado con parámetros configurables
    """
    logging.info("|- Iniciando orquestador de ingesta")
    
    if use_cache:
        logging.info("|- Usando versión con cache")
        df = get_db_vacunaciones_cached(since, until, chunk_size)
        
        # Limpiar la columna NUM_IDEN antes de persistir
        df = df.with_columns(
            df['NUM_IDEN'].str.replace_all("'", "").cast(str)
        )
        add_new_elements_to_lake('vacunacion', 'lk_vacunacion', ['NUM_IDEN', 'FECHA_APLICACION', 'UNICODIGO'], df)
    else:
        logging.info("|- Usando versión paralela con persistencia automática")
        # La función ya no retorna DataFrame, persiste directamente
        get_db_vacunaciones_parallel(since, until, chunk_size, max_workers)
        
        # Cargar datos desde el lago para obtener las identificaciones
        logging.info("|- Cargando datos desde el lago para procesamiento posterior")
        df = load_data()
    
    # datos del registro civil
    logging.info("|- Procesando datos del registro civil (MPI)")
    unique_identifiers = df['NUM_IDEN'].drop_nulls().drop_nans().unique().to_list()
    logging.info(f" |- Total de identificaciones únicas: {len(unique_identifiers):,}")
    
    mpi_df = get_mpi_data(unique_identifiers)
    add_new_elements_to_lake('vacunacion', 'lk_persona', ['IDENTIFIER_VALUE'], mpi_df)

    ## obtener datos geográficos
    logging.info("|- Procesando datos geográficos")
    geo_df = get_geo_salud_data()
    add_new_elements_to_lake('vacunacion', 'lk_establecimiento', ['uni_codigo'], geo_df)
    
    logging.info("|- Orquestador de ingesta completado")
    return df