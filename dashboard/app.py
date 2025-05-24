import streamlit as st
import pandas as pd
import redis
import json
from streamlit_autorefresh import st_autorefresh

r = redis.Redis(host="localhost", port=6379, db=0)

st.set_page_config(page_title="Accidentes en Tiempo Real", layout="wide")
st.title("Dashboard de Accidentes de Tránsito 🚦")

st_autorefresh(interval=2000, key="refresh")

records = r.lrange("accidents_data", 0, -1)
data = [json.loads(msg) for msg in records]

if not data:
    st.info("Esperando datos de Kafka vía Redis...")
else:
    df = pd.DataFrame(data)

    # Filtro por año si existe
    if "crash_year" in df.columns:
        available_years = sorted(df["crash_year"].dropna().unique())
        selected_years = st.multiselect("Selecciona uno o varios años:", available_years, default=available_years)
        df = df[df["crash_year"].isin(selected_years)]

    st.markdown("### 🆕 Últimos Accidentes")
    st.dataframe(df.tail(10), use_container_width=True)

    
    col1, col2 = st.columns(2)

    with col1:
        if "day_week_name" in df.columns:
            st.markdown("#### 🚗 Accidentes por Día de la Semana")
            days_order = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
            day_counts = df["day_week_name"].str.lower().value_counts().reindex(days_order).fillna(0)
            day_counts.index = [day.capitalize() for day in day_counts.index]
            if not day_counts.empty:
                st.bar_chart(day_counts)
            else:
                st.warning("No hay datos para los días de la semana.")

        if "month_name" in df.columns:
            st.markdown("#### 📅 Accidentes por Mes")
            months_order = ["january", "february", "march", "april", "may", "june",
                            "july", "august", "september", "october", "november", "december"]
            month_counts = df["month_name"].str.lower().value_counts().reindex(months_order).fillna(0)
            month_counts.index = [month.capitalize() for month in month_counts.index]
            st.bar_chart(month_counts)

    with col2:
        if "crash_year" in df.columns:
            st.markdown("#### 📈 Accidentes por Año")
            year_counts = df["crash_year"].value_counts().sort_index()
            st.line_chart(year_counts)

        if "crash_hour" in df.columns:
            st.markdown("#### ⏰ Accidentes por Hora")
            hour_counts = df["crash_hour"].value_counts().sort_index()
            st.bar_chart(hour_counts)

    st.markdown("---")

    st.markdown("### 🌧️ Accidentes por Condición Climática")
    if "weather_condition" in df.columns:
        weather_counts = df["weather_condition"].value_counts()
        st.bar_chart(weather_counts)

    st.markdown("### 💡 Accidentes por Condición de Iluminación")
    if "lighting_condition" in df.columns:
        light_counts = df["lighting_condition"].value_counts()
        st.bar_chart(light_counts)

    col3, col4 = st.columns(2)

    with col3:
        st.markdown("### 🚑 Accidentes con Heridos")
        if "injuries_total" in df.columns:
            injured_count = (df["injuries_total"] > 0).sum()
            st.metric(label="Total con heridos", value=injured_count)

    with col4:
        st.markdown("### ⚰️ Accidentes con Muertes")
        if "injuries_fatal" in df.columns:
            fatal_count = (df["injuries_fatal"] > 0).sum()
            st.metric(label="Total con muertos", value=fatal_count)