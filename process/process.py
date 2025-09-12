import polars

from process.dim_persona import persona_orchester


def process_orchester(df: DataFrame):
    persona_orchester(df)