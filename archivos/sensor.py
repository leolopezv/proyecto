import json
import random
import time
from kafka import KafkaProducer
################################################3

# Configuración del productor de Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',  # Dirección del broker de Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Nombre del topic de Kafka
topic = 'clima_en_guayaquil'

# Función para generar datos aleatorios
def generate_random_climate_data():
    temperature = round(random.uniform(19.0, 33.0), 1)  # Rango de temperaturas en °C para Guayaquil
    humidity = round(random.uniform(60.0, 95.0), 1)  # Rango de humedad relativa en Guayaquil
    return {
        "temperature": temperature,
        "humidity": humidity
    }

def generate_and_send_climate_data():
    while True:
        data = generate_random_climate_data()
        producer.send('clima-en-guayaquil', data)
        print(f"Sent data: {data}")
        time.sleep(random.uniform(1, 5))

if __name__ == "__main__":
    generate_and_send_climate_data()