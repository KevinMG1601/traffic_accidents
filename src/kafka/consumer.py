import json
from kafka import KafkaConsumer
import redis

KAFKA_TOPIC = "accidents_topic"
KAFKA_BOOTSTRAP = "localhost:29092"  
REDIS_HOST = "localhost"
REDIS_PORT = 6379

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)

consumer = KafkaConsumer(
    'accidents_topic',
    bootstrap_servers='localhost:29092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='csv-consumer-group'
)

print("Esperando mensajes...")

for msg in consumer:
    data = msg.value
    r.rpush("accidents_data", json.dumps(data))  
    print("Guardado en Redis:", data)
