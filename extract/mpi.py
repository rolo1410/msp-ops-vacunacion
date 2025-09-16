import logging

import polars as pl

from extract.config.sources import DB_MIP, get_oracle_engine


def get_mpi_data_chunk(identifications: list[str]) -> pl.DataFrame:
    db_mpi_data = get_oracle_engine(DB_MIP)
    query = f"""
            SELECT
                EC_IDENTIFIER_OID,
                IDENTIFIER_VALUE,
                GENDER,
                BIRTHDATE,
                MARITAL_STATUS,
                EC_FAMILY_GROUP,
                EC_SON_NUMBER,
                EC_ETHNICITY,
                NAME_USE,
                NAME_TEXT,
                NAME_FAMILY,
                NAME_GIVEN
            FROM
                MPI.PERSON
            WHERE
                EC_IDENTIFIER_OID IN ({','.join(map(repr, identifications))})
            AND
            ROWNUM < 10
            """
    df = pl.read_database(query, connection=db_mpi_data.connect())
    return df


def get_mpi_data(identifications: list[str]) -> pl.DataFrame:
    logging.info(f"|- MPI Obteniendo datos del MPI para {len(identifications)} identificaciones")
    chunk_size = 999
    dfs = []
    # por configuracion solo se puede traer en chunks de 999
    for i in range(0, len(identifications), chunk_size):
        logging.info(f" |- Fetching chunk {i // chunk_size + 1} de {(len(identifications) - 1) // chunk_size + 1}")
        chunk = identifications[i:i + chunk_size]
        df = get_mpi_data_chunk(chunk)
        dfs.append(df)
    df = pl.concat(dfs)
    return df
