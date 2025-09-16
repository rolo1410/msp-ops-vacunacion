import polars as pl


def remove_extra_whitespaces(df: pl.DataFrame,
                             columns: list[str]) -> pl.DataFrame:
    """
    Remove extra whitespaces from the text.

    Args:
        df (polars.DataFrame): The input DataFrame.

    Returns:
        polars.DataFrame: The cleaned DataFrame.
    """
    return df.with_columns(
        [pl.col(column).str.strip_chars() for column in df.columns if df[column].dtype == pl.Utf8]
    )
