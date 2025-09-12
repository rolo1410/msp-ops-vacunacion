import polars


def date_transform(df: DataFrame, column:str, format:str)->DataFrame:
    df = df.with_columns(
        polars.col(column).str.strptime(polars.Date,
                                        fmt=format).alias(column)
    )
    return df