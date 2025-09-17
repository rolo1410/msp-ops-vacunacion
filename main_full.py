from dotenv import load_dotenv

from extract.ingest_orchester import ingest_orchester
from lake.init_lake import add_new_elements_to_lake
from process.clean_transform_orchester import process_orchester

load_dotenv(override= True) 
## config loggin
import logging

## login in console and file
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# Crear un handler para archivo
file_handler = logging.FileHandler('app.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

# Agregar el handler de archivo al logger root
logging.getLogger().addHandler(file_handler)

##
if __name__ == "__main__":
    since = "1800-01-01"
    until = "2022-01-01" 
    ingest_orchester(since, until)
    df = process_orchester()
    add_new_elements_to_lake('vacunacion', 'db_vacunacion',['NUM_IDEN', 'FECHA_APLICACION', 'UNICODIGO'], df)