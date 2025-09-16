import polars as pl

from extract.config.sources import DB_MIP, get_oracle_engine


def get_mpi_data_chunk(identifications: list[str]) -> pl.DataFrame:
    db_mpi_data = get_oracle_engine(DB_MIP)
    query = f"""
            SELECT
                EC_IDENTIFIER_OID,
                GENDER,
                BIRTHDATE,
                MARITAL_STATUS,
                EC_FAMILY_GROUP,
                EC_SON_NUMBER,
                EC_ETHNICITY
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
    chunk_size = 1000
    dfs = []
    for i in range(0, len(identifications), chunk_size):
        chunk = identifications[i:i + chunk_size]
        df = get_mpi_data_chunk(chunk)
        dfs.append(df)
    df = pl.concat(dfs)
    return df
