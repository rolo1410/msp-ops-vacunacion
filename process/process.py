import polars as pl

from process.dim_persona import persona_orchester


def process_orchester(df: pl.DataFrame):
    persona_orchester(df)