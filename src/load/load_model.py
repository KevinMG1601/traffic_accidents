import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text
from src.connection.connection import create_engine_connection

def load_model_data(**kwargs):
    ti = kwargs["ti"]
    merged_json = ti.xcom_pull(key="merged_data")
    df = pd.read_json(merged_json)

    engine: Engine = create_engine_connection("DB_MODEL")


    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM hechos_accidentes"))
        if result.scalar() > 0:
            print("Los datos ya están cargados en 'hechos_accidentes'. Se omite la carga.")
            return

    def create_dim(df, col):
        return df[[col]].drop_duplicates().rename(columns={col: "descripcion"}).reset_index(drop=True)

    dim_dia_semana = df[["day_week_name"]].drop_duplicates().rename(columns={"day_week_name": "nombre_dia"}).reset_index(drop=True)
    dim_mes = df[["month_name"]].drop_duplicates().rename(columns={"month_name": "nombre_mes"}).reset_index(drop=True)
    dim_weather = create_dim(df, "weather_condition")
    dim_lighting = create_dim(df, "lighting_condition")
    dim_crash = create_dim(df, "crash_type")
    dim_first_crash = create_dim(df, "first_crash_type")

    def create_index_map(df, col):
        unique_vals = df[[col]].drop_duplicates().reset_index(drop=True)
        return {val: i+1 for i, val in enumerate(unique_vals[col])}

    map_dia = create_index_map(df, "day_week_name")
    map_mes = create_index_map(df, "month_name")
    map_weather = create_index_map(df, "weather_condition")
    map_lighting = create_index_map(df, "lighting_condition")
    map_crash = create_index_map(df, "crash_type")
    map_first_crash = create_index_map(df, "first_crash_type")

    df["dia_id"] = df["day_week_name"].map(map_dia)
    df["mes_id"] = df["month_name"].map(map_mes)
    df["weather_id"] = df["weather_condition"].map(map_weather)
    df["lighting_id"] = df["lighting_condition"].map(map_lighting)
    df["crash_type_id"] = df["crash_type"].map(map_crash)
    df["first_crash_id"] = df["first_crash_type"].map(map_first_crash)

    dim_tiempo = df[[
        "crash_hour", "crash_day_of_week", "dia_id",
        "crash_day_of_month", "crash_month", "mes_id", "crash_year"
    ]].drop_duplicates().reset_index(drop=True)
    dim_tiempo["fecha_id"] = range(1, len(dim_tiempo) + 1)
    df = df.merge(dim_tiempo, on=[
        "crash_hour", "crash_day_of_week", "dia_id",
        "crash_day_of_month", "crash_month", "mes_id", "crash_year"
    ])

    dim_clima = df[["weather_id", "lighting_id"]].drop_duplicates().reset_index(drop=True)
    dim_clima["clima_id"] = range(1, len(dim_clima) + 1)
    df = df.merge(dim_clima, on=["weather_id", "lighting_id"])

    dim_evento = df[["crash_type_id", "first_crash_id", "intersection_related_i"]].drop_duplicates().reset_index(drop=True)
    dim_evento["evento_id"] = range(1, len(dim_evento) + 1)
    df = df.merge(dim_evento, on=["crash_type_id", "first_crash_id", "intersection_related_i"])

    hechos = df[[
        "fecha_id", "clima_id", "evento_id", "num_units",
        "injuries_fatal", "injuries_total"
    ]]

    with engine.begin() as conn:
        dim_dia_semana.to_sql("dim_dia_semana", conn, if_exists="append", index=False)
        dim_mes.to_sql("dim_mes", conn, if_exists="append", index=False)
        dim_weather.to_sql("dim_weather_condition", conn, if_exists="append", index=False)
        dim_lighting.to_sql("dim_lighting_condition", conn, if_exists="append", index=False)
        dim_crash.to_sql("dim_crash_type", conn, if_exists="append", index=False)
        dim_first_crash.to_sql("dim_first_crash_type", conn, if_exists="append", index=False)
        dim_tiempo.to_sql("dim_tiempo", conn, if_exists="append", index=False)
        dim_clima.to_sql("dim_clima", conn, if_exists="append", index=False)
        dim_evento.to_sql("dim_evento", conn, if_exists="append", index=False)
        hechos.to_sql("hechos_accidentes", conn, if_exists="append", index=False, chunksize=10000)

    print(f"Carga finalizada. {len(hechos)} hechos insertados.")
