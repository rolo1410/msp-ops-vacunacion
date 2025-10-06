import logging

from extract.db_vacunacion_covid import load_lake_db_vacunacion_covid
from extract.db_vacunacion_rutinario import load_lake_db_vacunacion_rutinario
from extract.geo_salud import get_geo_salud_data
from extract.mpi import get_mpi_data
from lake.init_lake import add_new_elements_to_lake
from lake.load_lake import get_identificaciones_data


def ingest_vacunacion(since, until, chunk_size=500000, max_workers=4, use_cache=True):
    load_lake_db_vacunacion_rutinario(since, until, chunk_size, max_workers, use_cache)
    
    # Cargar datos desde el lago para obtener las identificaciones
    logging.info("|- Cargando datos desde el lago para procesamiento posterior")
    df = get_identificaciones_data('vacunacion_esquema', 'lk_vacunacion_rutinario', 'NUMEROIDENTIFICACION')
    
    # datos del registro civil
    logging.info("|- Procesando datos del registro civil (MPI)")
    unique_identifiers = df['num_iden'].drop_nulls().drop_nans().unique().to_list()
    logging.info(f" |- Total de identificaciones únicas: {len(unique_identifiers):,}")
    
    mpi_df = get_mpi_data(unique_identifiers)
    add_new_elements_to_lake('vacunacion_esquema', 'lk_persona', ['IDENTIFIER_VALUE'], mpi_df)

    ## obtener datos geográficos
    logging.info("|- Procesando datos geográficos")
    geo_df = get_geo_salud_data()
    add_new_elements_to_lake('vacunacion_esquema', 'lk_establecimiento', ['uni_codigo'], geo_df)
    
    
    
def ingest_vacunacion_covid(since, until, chunk_size=1000000):
    """
    Orquestador de ingesta optimizado con parámetros configurables
    """
    logging.info("|- Usando versión paralela con persistencia automática")
    
    # La función ya no retorna DataFrame, persiste directamente en una base de datos duckdb
    load_lake_db_vacunacion_covid(since, until, chunk_size)
    
    ## obtiene los datos de vacunación de rutina
    ##get_db_vacunaciones_parallel_rutinario(since, until, chunk_size, max_workers)
    
    # Cargar datos desde el lago para obtener las identificaciones
    logging.info("|- Cargando datos desde el lago para procesamiento posterior")
    df = get_identificaciones_data('vacunacion', 'lk_vacunacion_covid', 'num_iden')
    print(df.head())  # IGNORE
    
    # datos del registro civil
    logging.info("|- Procesando datos del registro civil (MPI)")
    unique_identifiers = df['num_iden'].drop_nulls().drop_nans().unique().to_list()
    logging.info(f" |- Total de identificaciones únicas: {len(unique_identifiers):,}")
    
    mpi_df = get_mpi_data(unique_identifiers)
    add_new_elements_to_lake('vacunacion', 'lk_persona', ['IDENTIFIER_VALUE'], mpi_df)

    ## obtener datos geográficos
    logging.info("|- Procesando datos geográficos")
    geo_df = get_geo_salud_data()
    add_new_elements_to_lake('vacunacion', 'lk_establecimiento', ['uni_codigo'], geo_df)
    
    logging.info("|- Orquestador de ingesta completado")

def ingest_orchester(since, until, chunk_size=1000000):
    ingest_vacunacion_covid(since, until, chunk_size)
    ##
    ##ingest_vacunacion(since, until, chunk_size)