import polars as pl

from extract.config.sources import DB_GEOSALUD, get_oracle_engine


def get_gosalud_data():
    db_geo_salud_data = get_oracle_engine(DB_GEOSALUD)
    query = f"""
            SELECT
                UNI_CODIGO,
                UNI_NOMBRE,
                PRV_CODIGO,
                PRV_DESCRIPCION,
                CAN_CODIGO,
                CAN_DESCRIPCION,
                PAR_CODIGO,
                PAR_DESCRIPCION,
                TIPO_ENTIDAD,
                MAIL,
                TIPO_ESTABLECEMIENTO,
                LONGPS,
                LATGPS,
                TIPO_ATENCION,
                ZONADEFRONTERA
            FROM
                SYAPP.VM_ESTABLECIMIENTOS_INGRESADOS
            """
    df = pl.read_database(query, connection=db_geo_salud_data.connect())
    return df