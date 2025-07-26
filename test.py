from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
import json
import time

# --------------------------------------
# CONFIGURACIÓN
# --------------------------------------
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'test-topic'
MONGO_URI = 'mongodb://localhost:27017'
MONGO_DB = 'streaming_test'
MONGO_COLLECTION = 'mensajes'

# --------------------------------------
# CONEXIÓN A MONGODB
# --------------------------------------
print("Conectando a MongoDB...")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
collection = db[MONGO_COLLECTION]
print("✅ Conectado a MongoDB.")

# --------------------------------------
# PRODUCTOR KAFKA
# --------------------------------------
print("Conectando como productor Kafka...")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
print("✅ Productor Kafka listo.")

# Enviar un mensaje
mensaje = {"msg": "Hola desde Kafka!", "time": time.time()}
producer.send(TOPIC, mensaje)
producer.flush()
print(f"✅ Mensaje enviado al topic '{TOPIC}': {mensaje}")

# --------------------------------------
# CONSUMIDOR KAFKA
# --------------------------------------
print("Conectando como consumidor Kafka...")
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
print("✅ Consumidor Kafka listo. Esperando mensajes...")

# Leer un mensaje y guardarlo en Mongo
for msg in consumer:
    print(f"✅ Mensaje recibido desde Kafka: {msg.value}")

    # Insertar en MongoDB
    collection.insert_one(msg.value)
    print("✅ Mensaje insertado en MongoDB.")

    # Salir después de procesar un mensaje
    break

print("🎉 ¡Todo probado correctamente!")