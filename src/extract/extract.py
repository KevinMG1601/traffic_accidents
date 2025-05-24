import os
import pandas as pd
from src.connection.connection import create_engine_connection

def extract_data(db_key: str) -> pd.DataFrame:
    """
    Extrae los datos desde la base de datos especificada (RAW, CLEAN o MODEL).

    Args:
        db_key (str): Clave de la base de datos (DB_RAW, DB_CLEAN o DB_MODEL)

    Returns:
        pd.DataFrame: Datos extraídos como DataFrame
    """
    engine = create_engine_connection(db_key)

    if db_key == "DB_RAW":
        table_name = "accidents"
    elif db_key == "DB_CLEAN":
        table_name = "accidents_clean"
    else:
        return None

    with engine.connect() as conn:
        df = pd.read_sql(f"SELECT * FROM {table_name}", con=conn)

    return df


def extract_api_task(**kwargs):
    """
    Extrae los datos del CSV limpio proveniente de la API desde ruta relativa
    y los pasa por XCom.
    """
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    csv_path = os.path.join(project_root, 'data', 'api', 'accidents_clean.csv')

    df = pd.read_csv(csv_path)
    df_json = df.to_json(orient="records")
    kwargs['ti'].xcom_push(key='api_data', value=df_json)

    print(f"Extraídos {len(df)} registros desde {csv_path}")


def extract_db_task(**kwargs):
    """
    Extrae los datos de la base DB_CLEAN y los pasa por XCom.
    """
    df = extract_data("DB_CLEAN")
    df_json = df.to_json(orient="records")
    kwargs['ti'].xcom_push(key='clean_data', value=df_json)

    print(f"Extraídos {len(df)} registros desde DB_CLEAN")
