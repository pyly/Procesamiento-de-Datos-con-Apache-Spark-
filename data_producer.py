# urban_data_producer.py

import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

class UrbanDataProducer:
    """
    Clase para generar y enviar datos de ruido y CO₂ a un topic de Kafka.
    """

    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        # Configuración de las ubicaciones simuladas
        self.locations = {
            1: {"name": "Zona Industrial", "noise_range": (70, 100), "co2_range": (400, 600)},
            2: {"name": "Zona Residencial", "noise_range": (30, 50), "co2_range": (300, 400)},
            3: {"name": "Zona Comercial", "noise_range": (50, 80), "co2_range": (350, 450)}
        }

    def generate_sensor_data(self, location_id):
        config = self.locations[location_id]
        noise_level = round(random.uniform(*config["noise_range"]), 2)
        co2_level = round(random.uniform(*config["co2_range"]), 2)
        return {
            "location_id": location_id,
            "location_name": config["name"],
            "timestamp": datetime.now().isoformat(),
            "noise_level": noise_level,
            "co2_level": co2_level
        }

    def run(self):
        print("Iniciando productor de datos de ambiente urbano...")
        try:
            while True:
                for location_id in self.locations.keys():
                    data = self.generate_sensor_data(location_id)
                    self.producer.send('urban_data', value=data)
                    print(f"Enviado: {json.dumps(data, indent=2)}")
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nDeteniendo el productor...")
            self.producer.close()

if __name__ == "__main__":
    producer = UrbanDataProducer()
    producer.run()
