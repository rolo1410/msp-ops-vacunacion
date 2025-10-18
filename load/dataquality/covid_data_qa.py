import os
import json
from datetime import datetime
import pandas as pd
import great_expectations as ge

# Intentar importar el profiler; ajustar según la versión de GE si es necesario
try:
    from great_expectations.profile.basic_suite_builder import BasicSuiteBuilderProfiler
except Exception:
    raise ImportError("No se pudo importar BasicSuiteBuilderProfiler desde great_expectations.profile.basic_suite_builder. Verifique la versión de great_expectations.")


def _ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def configure_and_profile(
    df: pd.DataFrame,
    suite_name: str = "covid_auto_suite",
    output_root: str = "./great_expectations",
):
    """
    Genera una Expectation Suite a partir de un DataFrame, valida el dataset
    y guarda la suite y el resultado de validación en disco.

    Parámetros:
        df: pandas.DataFrame a validar
        suite_name: nombre base para la Expectation Suite (se crea archivo <suite_name>.json)
        output_root: carpeta raíz para guardar expectations y validations
    """
    # Validar tipo
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df debe ser un pandas.DataFrame")

    # Crear objeto GE a partir del DataFrame
    ge_df = ge.from_pandas(df)

    # Generar suite con el profiler básico
    profiler = BasicSuiteBuilderProfiler()
    suite = profiler.profile(ge_df)

    # Normalizar nombre/paths
    expectations_dir = os.path.join(output_root, "expectations")
    validations_dir = os.path.join(output_root, "validations")
    _ensure_dir(expectations_dir)
    _ensure_dir(validations_dir)

    # Guardar Expectation Suite a JSON
    suite_filename = f"{suite_name}.json"
    suite_path = os.path.join(expectations_dir, suite_filename)
    with open(suite_path, "w", encoding="utf-8") as f:
        json.dump(suite.to_json_dict(), f, indent=2, ensure_ascii=False)

    # Validar el dataset contra la suite generada
    validation_result = ge_df.validate(expectation_suite=suite, result_format="SUMMARY")

    # Guardar resultado de validación
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    validation_filename = f"{suite_name}_validation_{timestamp}.json"
    validation_path = os.path.join(validations_dir, validation_filename)

    try:
        validation_json = validation_result.to_json_dict()
    except Exception:
        validation_json = validation_result

    with open(validation_path, "w", encoding="utf-8") as f:
        json.dump(validation_json, f, indent=2, ensure_ascii=False)

    # Resumen simple por consola
    successful = validation_result["success"] if isinstance(validation_result, dict) else getattr(validation_result, "success", None)
    summary = {
        "suite_path": suite_path,
        "validation_path": validation_path,
        "success": successful,
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return summary


if __name__ == "__main__":
    # Ejemplo de uso: leer CSV y pasar el DataFrame a la función
    CSV_PATH = "/path/to/your/covid_dataset.csv"  # <- Cambiar por la ruta real
    df = pd.read_csv(CSV_PATH)
    configure_and_profile(df)
