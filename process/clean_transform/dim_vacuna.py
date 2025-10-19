import logging
import polars as pl


def _vacunas_nombres(df):
    logging.info("|- ENR Agregando nombres de vacunas")
    # homologar valores en funcion del csv de homologacion
    vacunas_nombre_comercial = pl.read_csv("resources/homologations/vacunas_nombre_comercial.csv")
    # left join con el dataframe original
    
    df = df.join(vacunas_nombre_comercial, left_on="nombre_vacuna", right_on="valor_original", suffix="_map" , how="left")
    df = df.with_columns(pl.col("valor_homologado").alias("nombre_vacuna_comercial"))
    # eliminar espacion en blanco al inicio y al fin de valor homologado
    df = df.with_columns(pl.col("nombre_vacuna_comercial").str.strip_chars())
    
    return df
    
def _descomponer_fecha_vacunacion(df):
    logging.info("|- ENR Descomponiendo fecha de vacunación")
    ## separar la columna fecha vacunacion en dia, mes, año
    df = df.with_columns(
        pl.col("fecha_aplicacion").dt.day().alias("dia_aplicacion"),
        pl.col("fecha_aplicacion").dt.month().alias("mes_aplicacion"),
        pl.col("fecha_aplicacion").dt.year().alias("anio_aplicacion")
    )
    return df

def vacuna_orchester(df):
    print(f"Dim vacuna - columnas: {len(df)}")
    df = _vacunas_nombres(df)
    df = _descomponer_fecha_vacunacion(df)
    print(f"Dim vacuna - columnas: {len(df)}")
    return df