from confluent_kafka import Producer
import time
import json
import csv
import os


# Función para el callback de entrega de mensajes
def delivery_report(err, msg):
    if err is not None:
        print(f"Error en la entrega: {err}")
    else:
        msg_value = json.loads(msg.value().decode("utf-8"))
        product_name = msg_value.get(
            "product_name", "Nombre del producto no encontrado"
        )
        print(
            f"Mensaje entregado a {msg.topic()} [{msg.partition()}] para el producto {product_name}"
        )


# Configuración del productor
conf = {"bootstrap.servers": "localhost:9093"}

# Creación del productor
producer = Producer(conf)

# Leer el archivo CSV y enviar filas al topic 'transactions'
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_file = os.path.join(script_dir, "../../../../datasets/E2/amazon.csv")
csv_file = os.path.abspath(csv_file)
with open(csv_file, mode="r", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Convertir la fila a JSON
        json_message = json.dumps(row)

        # Enviar mensaje al topic 'transactions'
        producer.produce(
            "transactions", json_message.encode("utf-8"), callback=delivery_report
        )
        producer.poll(0)  # Activar el callback

# Esperar a que se entreguen todos los mensajes pendientes
producer.flush()

print("Todos los mensajes han sido enviados con éxito.")
