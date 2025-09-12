import logging

from sqlalchemy import create_engine


def create_oracle_engine(config, type):
    try:
        engine = create_engine("postgresql+psycopg2://scott:tiger@localhost:5432/mydatabase")
        return engine
    except Exception as e:
        logging.info(f"Error al establecer la conexión con la base de datos: {e}")


def process_data(engine, since, until, chunk_size=10000):
    try:
        with engine.connect() as connection:
            result = connection.execute(
                "SELECT * FROM my_table WHERE date >= :since AND date < :until",
                {"since": since, "until": until}
            )
            for chunk in iter(lambda: list(itertools.islice(result, chunk_size)), []):
                process_chunk(chunk)
    except Exception as e:
        logging.info(f"Error al procesar los datos: {e}")       
