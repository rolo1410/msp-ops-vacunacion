from extract.db_vacunacion import get_db_vacunaciones, get_db_vacunaciones_parallel, get_db_vacunaciones_cached
from extract.geo_salud import get_geo_salud_data
from extract.mpi import get_mpi_data
from lake.init_lake import add_new_elements_to_lake


def ingest_orchester(since, until, chunk_size=500000, max_workers=4, use_cache=True):
    """
    Orquestador de ingesta optimizado con parámetros configurables
    """
    if use_cache:
        df = get_db_vacunaciones_cached(since, until, chunk_size)
    else:
        df = get_db_vacunaciones_parallel(since, until, chunk_size, max_workers)
    ## remover caracter ' especiales de la columna NUM_IDEN y castearla a string

    df.with_columns(
        df['NUM_IDEN'].str.replace_all("'", "").cast(str)
    )
    add_new_elements_to_lake('vacunacion', 'lk_vacunacion', ['NUM_IDEN', 'FECHA_APLICACION', 'UNICODIGO'], df)
    
    # datos del registro civil
    mpi_df = get_mpi_data(df['NUM_IDEN'].drop_nulls().drop_nans().unique().to_list())
    add_new_elements_to_lake('vacunacion', 'lk_persona', ['IDENTIFIER_VALUE'], mpi_df)

    ## obtener datos geográficos
    geo_df = get_geo_salud_data()
    add_new_elements_to_lake('vacunacion', 'lk_establecimiento', ['uni_codigo'], geo_df)
    
    return df