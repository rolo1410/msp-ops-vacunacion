from dotenv import load_dotenv

from extract.ingest_orchester import ingest_orchester

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
    since = "2023-03-01"
    until = "2023-03-06" 
    ingest_orchester(since, until)