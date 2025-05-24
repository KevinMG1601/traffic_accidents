from kafka import KafkaProducer
from json import dumps
import pandas as pd
import time

def send_to_kafka_task(**kwargs):
    from kafka.errors import KafkaTimeoutError

    ti = kwargs['ti']
    merged_json = ti.xcom_pull(key='merged_data')
    df = pd.read_json(merged_json)

    topic = "accidents_topic"
    broker = "localhost:29092"

    producer = KafkaProducer(
        value_serializer=lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers=[broker],
        batch_size=16384,         # Tamaño del buffer en bytes
        linger_ms=10,             # Espera antes de enviar (permite agrupar más)
        retries=5,
        request_timeout_ms=60000,
        max_block_ms=60000
    )

    batch_size = 50
    total_sent = 0

    print(f"🚛 Enviando mensajes a Kafka por lotes de {batch_size} registros...")

    for start in range(0, len(df), batch_size):
        end = start + batch_size
        batch = df.iloc[start:end].to_dict(orient='records')

        for record in batch:
            try:
                producer.send(topic, value=record)
                total_sent += 1
            except KafkaTimeoutError as e:
                print(f"[❌] Error al enviar: {e}")

        producer.flush()
        time.sleep(1)
        print(f"[✔️] Lote de {len(batch)} mensajes enviado.")

    producer.close()
    print(f"[✅] Total de mensajes enviados a Kafka: {total_sent}")
