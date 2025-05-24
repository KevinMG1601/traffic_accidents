import pandas as pd


def transform_api_data(**kwargs):
    """
    Transforma el CSV de la API para que coincida con el formato del dataset en DB_CLEAN.
    """
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='api_data')
    df = pd.read_json(df_json)

    df.rename(columns={
        've_total': 'num_units',
        'hour': 'crash_hour',
        'day_week': 'crash_day_of_week',
        'day_weekname': 'day_week_name',
        'month': 'crash_month',
        'monthname': 'month_name',
        'year': 'crash_year',
        'day': 'crash_day_of_month',
        'lgt_condname': 'lighting_condition',
        'weathername': 'weather_condition',
        'reljct1name': 'intersection_related_i',
        'fatals': 'injuries_fatal',
        'persons': 'injuries_total',
        'harm_evname': 'crash_type',
        'man_collname': 'first_crash_type'
    }, inplace=True)

    df["intersection_related_i"] = df["intersection_related_i"].map({
    "Yes": True,
    "No": False,
    "Unknown": False,
    "Not Reported": False
    }).fillna(False)

    weather_map_api = {
    "Clear": "clear",
    "Rain": "rain",
    "Snow": "snow",
    "Cloudy": "cloudy",
    "Fog, Smog, Smoke": "fog",
    "Blowing Snow": "blowing snow",
    "Freezing Rain or Drizzle": "freezing rain",
    "Other": "other",
    "Sleet or Hail": "sleet",
    "Severe Crosswinds": "crosswinds",
    "Blowing Sand, Soil, Dirt": "blowing sand",
    "Unknown": "unknown",
    "Not Reported": "unknown"
    }

    df["weather_condition"] = df["weather_condition"].map(weather_map_api).fillna("unknown")

    lighting_map_api = {
    "Daylight": "daylight",
    "Dark - Lighted": "dark_lighted",
    "Dark - Not Lighted": "dark_not_lighted",
    "Dawn": "dawn",
    "Dusk": "dusk",
    "Dark - Unknown Lighting": "unknown",
    "Unknown": "unknown",
    "Not Reported": "unknown",
    "Other": "unknown"
    }

    df["lighting_condition"] = df["lighting_condition"].map(lighting_map_api).fillna("unknown")

    first_crash_map_api = {
    "Angle": "angle",
    "Front-to-Rear": "rear_end",
    "Rear-to-Rear": "rear_to_rear",
    "Rear-to-Side": "rear_to_side",
    "Sideswipe - Same Direction": "sideswipe_same",
    "Sideswipe - Opposite Direction": "sideswipe_opposite",
    "Front-to-Front": "head_on",
    "Not a Collision with Motor Vehicle In-Transport": "non_collision",
    "Other": "other",
    "Not Reported": "other",
    "Unknown": "other"
    }

    df["first_crash_type"] = df["first_crash_type"].map(first_crash_map_api).fillna("other")

    crash_type_map_api = {
    "Motor Vehicle In-Transport": "vehicle",
    "Parked Motor Vehicle": "vehicle",
    "Working Motor Vehicle": "vehicle",
    "Tree (Standing Only)": "fixed_object",
    "Boulder": "fixed_object",
    "Culvert": "fixed_object",
    "Ditch": "fixed_object",
    "Utility Pole/Light Support": "fixed_object",
    "Guardrail Face": "fixed_object",
    "Guardrail End": "fixed_object",
    "Wall": "fixed_object",
    "Fence": "fixed_object",
    "Bridge Rail (Includes parapet)": "fixed_object",
    "Bridge Pier or Support": "fixed_object",
    "Bridge Overhead Structure": "fixed_object",
    "Concrete Traffic Barrier": "fixed_object",
    "Traffic Sign Support": "fixed_object",
    "Traffic Signal Support": "fixed_object",
    "Fire Hydrant": "fixed_object",
    "Impact Attenuator/Crash Cushion": "fixed_object",
    "Other Fixed Object": "fixed_object",
    "Other Traffic Barrier": "fixed_object",
    "Pedestrian": "non_motorist",
    "Pedalcyclist": "non_motorist",
    "Ridden Animal or Animal Drawn Conveyance": "non_motorist",
    "Live Animal": "non_motorist",
    "Rollover/Overturn": "rollover",
    "Jackknife (harmful to this vehicle)": "rollover",
    "Fire/Explosion": "other",
    "Unknown": "other",
    "Not Reported": "other",
    "Other Object (not fixed)": "other",
    "Other Non-Collision": "other",
    "Other": "other",
    "Cargo/Equipment Loss or Shift (harmful to this vehicle)": "other",
    "Object Fell From Motor Vehicle In-Transport": "other",
    "Object That Had Fallen From Motor Vehicle In-Transport": "other",
    "Fell/Jumped from Vehicle": "other",
    "Immersion or Partial Immersion": "other",
    "Pavement Surface Irregularity (Ruts, Potholes, etc.)": "other",
    "Thrown or Falling Object": "other",
    "Bridge-related": "fixed_object"
    }

    df["crash_type"] = df["crash_type"].map(crash_type_map_api).fillna("other")

    df["crash_hour"] = pd.to_numeric(df["crash_hour"], errors='coerce').fillna(0).astype(int)
    df["crash_hour"] = df["crash_hour"].replace(99, 0) 

    num_cols = [
        'num_units', 'crash_day_of_week', 'crash_month',
        'crash_year', 'crash_day_of_month', 'injuries_fatal', 'injuries_total'
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    df = df[[
        'num_units', 'crash_hour', 'crash_day_of_week', 'day_week_name',
        'crash_month', 'month_name', 'crash_year', 'crash_day_of_month',
        'lighting_condition', 'weather_condition', 'injuries_fatal',
        'intersection_related_i', 'injuries_total', 'crash_type', 'first_crash_type'
    ]]

    df_json = df.to_json(orient="records")
    ti.xcom_push(key='transformed_api_data', value=df_json)
    print(f"API transformada para merge con DB_CLEAN. Registros: {len(df)}")



def transform_db_data(**kwargs):
    """
    Transforma los datos extraídos desde DB_CLEAN para que sean compatibles con los datos transformados de la API.
    """
    ti = kwargs['ti']
    df_json = ti.xcom_pull(key='clean_data')
    df = pd.read_json(df_json)

    day_names = {
        1: "monday", 2: "tuesday", 3: "wednesday", 4: "thursday",
        5: "friday", 6: "saturday", 7: "sunday"
    }
    month_names = {
        1: "january", 2: "february", 3: "march", 4: "april", 5: "may", 6: "june",
        7: "july", 8: "august", 9: "september", 10: "october", 11: "november", 12: "december"
    }

    df["intersection_related_i"] = df["intersection_related_i"].map({'Y': True, 'N': False}).fillna(False)

    weather_map_db = {
    "CLEAR": "clear",
    "RAIN": "rain",
    "SNOW": "snow",
    "CLOUDY/OVERCAST": "cloudy",
    "FOG/SMOKE/HAZE": "fog",
    "BLOWING SNOW": "blowing snow",
    "FREEZING RAIN/DRIZZLE": "freezing rain",
    "OTHER": "other",
    "SLEET/HAIL": "sleet",
    "SEVERE CROSS WIND GATE": "crosswinds",
    "BLOWING SAND, SOIL, DIRT": "blowing sand",
    "UNKNOWN": "unknown"
    }

    df["weather_condition"] = df["weather_condition"].map(weather_map_db).fillna("unknown")

    lighting_map_db = {
    "DAYLIGHT": "daylight",
    "DARKNESS, LIGHTED ROAD": "dark_lighted",
    "DARKNESS": "dark_not_lighted",
    "DUSK": "dusk",
    "DAWN": "dawn",
    "UNKNOWN": "unknown"
    }

    df["lighting_condition"] = df["lighting_condition"].map(lighting_map_db).fillna("unknown")

    first_crash_map_db = {
    "TURNING": "other",
    "REAR END": "rear_end",
    "REAR TO FRONT": "rear_end",
    "REAR TO REAR": "rear_to_rear",
    "REAR TO SIDE": "rear_to_side",
    "ANGLE": "angle",
    "HEAD ON": "head_on",
    "SIDESWIPE SAME DIRECTION": "sideswipe_same",
    "SIDESWIPE OPPOSITE DIRECTION": "sideswipe_opposite",
    "PEDESTRIAN": "non_collision",
    "PEDALCYCLIST": "non_collision",
    "FIXED OBJECT": "non_collision",
    "PARKED MOTOR VEHICLE": "non_collision",
    "OTHER NONCOLLISION": "other",
    "OVERTURNED": "other",
    "OTHER OBJECT": "other",
    "ANIMAL": "non_collision",
    "TRAIN": "non_collision"
    }

    df["first_crash_type"] = df["first_crash_type"].map(first_crash_map_db).fillna("other")

    crash_type_map_db = {
    "REAR END": "vehicle",
    "HEAD ON": "vehicle",
    "ANGLE": "vehicle",
    "PEDESTRIAN": "non_motorist",
    "PEDALCYCLIST": "non_motorist",
    "FIXED OBJECT": "fixed_object",
    "PARKED MOTOR VEHICLE": "vehicle",
    "OVERTURNED": "rollover",
    "OTHER NONCOLLISION": "other",
    "OTHER OBJECT": "other",
    "ANIMAL": "non_motorist",
    "TRAIN": "other",
    "TURNING": "vehicle",
    "REAR TO REAR": "vehicle",
    "REAR TO SIDE": "vehicle",
    "SIDESWIPE SAME DIRECTION": "vehicle",
    "SIDESWIPE OPPOSITE DIRECTION": "vehicle"
    }
    df["crash_type"] = df["crash_type"].map(crash_type_map_db).fillna("other")

    num_cols = [
        'num_units', 'crash_hour', 'crash_day_of_week', 'crash_month',
        'crash_year', 'crash_day_of_month', 'injuries_fatal', 'injuries_total'
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    df["day_week_name"] = df["crash_day_of_week"].map(day_names).fillna("unknown").str.lower()
    df["month_name"] = df["crash_month"].map(month_names).fillna("unknown").str.lower()

    df = df[[
        'num_units', 'crash_hour', 'crash_day_of_week', 'day_week_name',
        'crash_month', 'month_name', 'crash_year', 'crash_day_of_month',
        'lighting_condition', 'weather_condition', 'injuries_fatal',
        'intersection_related_i', 'injuries_total', 'crash_type', 'first_crash_type'
    ]]

    df_json = df.to_json(orient='records')
    ti.xcom_push(key='transformed_db_data', value=df_json)
    print(f"DB_CLEAN transformada. Registros: {len(df)}")



def merge_data(**kwargs):
    """
    Une los datos transformados de la API y de DB_CLEAN en un solo DataFrame para carga posterior.
    """
    ti = kwargs['ti']
    api_json = ti.xcom_pull(key='transformed_api_data')
    clean_json = ti.xcom_pull(key='transformed_db_data')

    df_api = pd.read_json(api_json)
    df_clean = pd.read_json(clean_json)

    df_merged = pd.concat([df_clean, df_api], ignore_index=True)

    print(f"Merge realizado correctamente. Total registros: {len(df_merged)}")

    ti.xcom_push(key="merged_data", value=df_merged.to_json(orient="records"))

