import polars


def _es_calcular_fase(df: polars.DataFrame):
    logging.info("|- ENR Calculando fase de vacunación")
    # Lógica para calcular la fase de vacunación
    df = df.with_columns(
        pl.lit("Fase 1").alias("fase_vacunacion")  # Ejemplo simplificado
    )
    return df

def vacunacion_orchester(df: polars.DataFrame):
    return df