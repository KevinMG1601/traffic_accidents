import great_expectations as gx
import pandas as pd

def validate_data_with_gx(**kwargs) -> bool:
    ti = kwargs["ti"]
    json_data = ti.xcom_pull(key="merged_data")
    df = pd.read_json(json_data)

    context = gx.get_context(mode="ephemeral")

    suite = context.add_expectation_suite("accidents_expectations")

    validator = context.sources.pandas_default.read_dataframe(df)

    validator.expect_column_to_exist("crash_hour")
    validator.expect_column_values_to_be_between("crash_hour", min_value=0, max_value=23)
    validator.expect_column_values_to_not_be_null("day_week_name")
    validator.expect_column_values_to_be_in_set("weather_condition", [
        "clear", "rain", "snow", "cloudy", "fog", "blowing snow", "freezing rain",
        "other", "sleet", "crosswinds", "blowing sand", "unknown"
    ])

    results = validator.validate()

    if not results["success"]:
        raise ValueError("Falló la validación con Great Expectations.")

    print("Validación con GX completada correctamente.")
    return True
