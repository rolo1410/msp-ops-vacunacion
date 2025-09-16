import logging

from sqlalchemy import create_engine


def create_oracle_engine(config, type):
    try:
        engine = create_engine("postgresql+psycopg2://scott:tiger@localhost:5432/mydatabase")
        return engine
    except Exception as e:
        logging.info(f"Error al establecer la conexión con la base de datos: {e}")


def save_to_lake(data_chunk):
    # Implement the logic to save the data chunk to the lake
    logging.info(f"Saving chunk of size {len(data_chunk)} to the lake...")
    pass    


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


def ingest_data_lake(since, until, chunk_size=10000):
    logging.basicConfig(level=logging.INFO)
    logging.info("Iniciando el proceso de ingestión de datos al lago...")

    engine = create_oracle_engine("DB_CONFIG", "oracle")
    if not engine:
        logging.error("No se pudo crear el motor de la base de datos. Terminando el proceso.")
        return

    process_data(engine, since, until, chunk_size)
    logging.info("Proceso de ingestión de datos al lago completado.")