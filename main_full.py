from dotenv import load_dotenv

from extract.db_vacunacion import get_db_vacunaciones
from lake.ingest_lake import process_data

load_dotenv(override= True) 
## config loggin
import logging

## login in console and file
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Crear un handler para archivo
file_handler = logging.FileHandler('app.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

# Agregar el handler de archivo al logger root
logging.getLogger().addHandler(file_handler)

##
if __name__ == "__main__":
    since = "2023-01-01"
    until = "2023-03-31"
    get_db_vacunaciones(since, until, chunk_size=100000)