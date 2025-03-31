from kafka import KafkaProducer
import json
import pandas as pd

# Configuración de Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_send_success(record_metadata):
    print(f"Sent to {record_metadata.topic} | Partition {record_metadata.partition} | Offset {record_metadata.offset}")

def on_send_error(excp):
    print('Error sending message', exc_info=excp)

# URL del dataset
url = 'https://raw.githubusercontent.com/carlitaRP/kafka/refs/heads/main/results/motorcycle_sales/part-00000-280f54d0-d2ad-4206-832b-8a212b1850eb-c000.json'

# Cargar dataset
df = pd.read_json(url, lines=True)  # ✅ Importante: Agregar `lines=True` porque los datos están en formato JSONL

for _, row in df.head(100).iterrows():
    dict_data = row.to_dict()  # ✅ Convertir fila en diccionario
    producer.send('motorcycle', value=dict_data).add_callback(on_send_success).add_errback(on_send_error)
    print(f"Sent: {dict_data}")

producer.close()