# Project ETL - Traffic Accidents 🚗

## **Overview**
The objective of this project is to perform an in-depth exploratory analysis of the traffic accident dataset. The analysis seeks to identify patterns, time trends, geographic distribution, and relevant factors associated with the incidents, in order to generate knowledge that can support decision-making and prevention strategies.

## Dataset [Traffic Accidents Kaggle](https://www.kaggle.com/datasets/oktayrdeki/traffic-accidents)

* **crash_date:** The date the accident occurred.
* **traffic_control_device:** The type of traffic control device involved (e.g., traffic light, sign).
* **weather_condition:** The weather conditions at the time of the accident.
* **lighting_condition:** The lighting conditions at the time of the accident.
* **first_crash_type:** The initial type of the crash (e.g., head-on, rear-end).
* **trafficway_type:** The type of roadway involved in the accident (e.g., highway, local road).
* **alignment:** The alignment of the road where the accident occurred (e.g., straight, curved).
* **roadway_surface_cond:** The condition of the roadway surface (e.g., dry, wet, icy).
* **road_defect:** Any defects present on the road surface.
* **crash_type:** The overall type of the crash.
* **intersection_related_i:** Whether the accident was related to an intersection.
* **damage:** The extent of the damage caused by the accident.
* **prim_contributory_cause:** The primary cause contributing to the crash.
* **num_units:** The number of vehicles involved in the accident.
* **most_severe_injury:** The most severe injury sustained in the crash.
* **injuries_total:** The total number of injuries reported.
* **injuries_fatal:** The number of fatal injuries resulting from the accident.
* **injuries_incapacitating:** The number of incapacitating injuries.
* **injuries_non_incapacitating:** The number of non-incapacitating injuries.
* **injuries_reported_not_evident:** The number of injuries reported but not visibly evident.
* **injuries_no_indication:** The number of cases with no indication of injury.
* **crash_hour:** The hour the accident occurred.
* **crash_day_of_week:** The day of the week the accident occurred.
* **crash_month:** The month the accident occurred.

## **API Crash Viewer NHTSA**
![LOGO API](https://crashviewer.nhtsa.dot.gov/CrashAPI/Images/NHTSA_Logo_w_tag.png)

Para la API se descargo la data en formato `csv` directo desde la pagia porque al consumir la API era muy demorada al dar la informacion de todos los States, asi que se tomo es decision de utilizarla asi, este es el link de la pagina de la API: https://crashviewer.nhtsa.dot.gov/CrashAPI/


## **PIPELINE**
![Pipeline_ETL](/assets/airflow.png "This is a pipeline")


## Tools and Libraries
El proyecto utiliza diversas herramientas de software para construir el flujo ETL, procesar los datos y modelarlos para análisis posterior:

* [Python 3.11.9](https://www.python.org/downloads/release/python-3119/)
* [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
* [Jupyter Notebook](https://docs.jupyter.org/en/latest/)
* [Kafka](https://kafka.apache.org/documentation/)
* [Docker](https://docs.docker.com/)
* [MySQL](https://dev.mysql.com/downloads/installer/)
* [Redis](https://redis.io/docs/latest/)


## Installation and Setup
1. Clona el repositorio:
    ```bash
    git clone https://github.com/KevinMG1601/traffic_accidents.git
    cd Project_ETL
    ```

2. Crea y activa un entorno virtual:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3. Instala las dependencias:
    ```bash
    pip install -r requirements.txt

## Configuración

### 1. Configura el archivo `.env`
Crear el `.env` basado con el `.env.example` con tus variables.

`IMPORTANTE TENER CREADO LAS 3 BASES DE DATOS!!!!`

### 2. Docker  

Ejecutar el primer code:
```
docker compose up -d
```
![docker1](/assets/docker1.png)

Comprobamos los servicios con:
```
docker ps
```
![docker2](/assets/docker2.png)

### 3. Consumer

Ejecutamos el consumer con:
```
python src/kafka/consumer.py
```
El consumer va a quedar en espera a recibir los mensajes del producer.

### 4. Dashboard Real Time

Ejecutamos el dashboard que va a recibir los datos del consumer:
```
streamlit run dashboard/app.py
```
En el navegador vamos al enlace localhost con el puerto que nos da.
![dashboard](/assets/dash.png)

### 5. Airflow 

Ejecutar el primer code:
```
export AIRFLOW_HOME="$(pwd)/airflow”
```

Despues ejecute:
```
airflow standalone 
```
vaya a **`localhost:8080`** e inicia sesion con el password y user que te da airflow.

Por ultimo Ejecuta el DAG llamado `accidents_etl`.

### **VIDEO**
Este es el vide de demostracion: https://drive.google.com/drive/folders/110HVvHGUUr82vZHhTtQWflbI6nfqE3IL?usp=sharing


## Modelo dimensional

![modelo_dimensional](/assets/modelo_dimensional.png)

## **CONCLUSIONES**
Este proyecto logró integrar una tubería completa de tecnología de datos que automatiza la extracción, transformación, validación, almacenamiento y visualización de tiempo real con accidentes de tráfico. 

Utilizando tecnologías como Apache Air Flow to Orchestra, Kafka sobre la transmisión de datos de transmisión, Redis como la visualización intermedia de almacenamiento y flujo se convirtió en una arquitectura estable y escalable consolidada. 

Desde un punto de vista funcional, se desarrollaron una serie de transformaciones para homogeneizar datos de dos fuentes diferentes (una base almacenada en MySQL y API externa), lo que permite que sea efectivo. Con grandes esperanzas, se incorporó la capa de validación de calidad de datos para garantizar que los conjuntos utilizados cumplan ciertos estándares. 

Finalmente, el uso de un panel de información dinámico e interactivo ofrece visualizaciones actualizadas cada pocos segundos, lo que le permite observar la ocurrencia de accidentes por día. Día, hora, mes, año, estado climático o de iluminación y severidad. Proporciona una plataforma potencial para decisiones de tiempo real y accidentes críticos de la ciudad. Este flujo de datos automatizado no solo muestra habilidades técnicas en el procesamiento de datos y el desarrollo de la tubería, sino que también lo considera una base para soluciones analíticas relacionadas con la seguridad vial, el urbanismo inteligente y la prevención de riesgos.


## **AUTHOR**
<table style="border-collapse: collapse; border: none;">
  <tr>
    <td align="center" width="150" style="border: none;">
      <a href="https://github.com/KevinMG1601">
        <img src="https://avatars.githubusercontent.com/u/143461336?v=4" width="100px" alt="Kevin Muñoz"/><br />
        <span style="color: black; font-weight: bold;">Kevin Muñoz</span>
      </a>
    </td>
    <td style="border: none; vertical-align: top;">
      Created by <b>Kevin Muñoz</b>. I would like to know your opinion about this project. You can write me by <a href="mailto:kevin.andres2636@gmail.com">email</a> or connect with me on <a href="https://www.linkedin.com/in/kevin-mu%C3%B1oz-231b80303/">LinkedIn</a>.
    </td>
  </tr>
</table>