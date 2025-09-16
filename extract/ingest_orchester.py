from extract.db_vacunacion import get_db_vacunaciones
from extract.geo_salud import get_geo_salud_data
from extract.mpi import get_mpi_data


def ingest_orchester(since, until):
    df = get_db_vacunaciones(since, until)
    ## remover caracter ' especiales de la columna NUM_IDEN y castearla a string

    df.with_columns(
        df['NUM_IDEN'].str.replace_all("'", "").cast(str)
    )
    mpi_df = get_mpi_data(df['NUM_IDEN'].drop_nulls().drop_nans().unique().to_list())
    print(mpi_df)

    ## obtener datos geográficos
    geo_df = get_geo_salud_data()
    print(geo_df)   
    # merge con mpi
    df = df.join(mpi_df, left_on='NUM_IDEN', right_on='EC_IDENTIFIER_OID', how='left')
    print(df)
    # merge con geo
    df = df.join(geo_df, left_on='UNICODIGO', right_on='UNI_CODIGO', how='left')
    print(df)
    return df