"""Configuración compartida entre productor y consumidor

Las credenciales NO están en el código: se leen del archivo .env
(que está en .gitignore).
"""

import os

from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "161.35.11.141:9092")

SECURITY_PROTOCOL = "SASL_PLAINTEXT"
SASL_MECHANISM = "PLAIN"
SASL_USERNAME = os.getenv("KAFKA_USERNAME")
SASL_PASSWORD = os.getenv("KAFKA_PASSWORD")

if not SASL_USERNAME or not SASL_PASSWORD:
    raise SystemExit(
        "Faltan KAFKA_USERNAME o KAFKA_PASSWORD. "
    )

TOPIC_JSON = "22246"             # Partes 3.2 y 3.3
TOPIC_COMPACT = "22246-compact"  # Parte 3.4 (payload de 3 bytes)

KEY = "sensor1"
GROUP_ID = "22246-consumer"      # Lo usa el consumidor

# Parámetros comunes para KafkaProducer / KafkaConsumer (kafka-python)
KAFKA_AUTH = dict(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol=SECURITY_PROTOCOL,
    sasl_mechanism=SASL_MECHANISM,
    sasl_plain_username=SASL_USERNAME,
    sasl_plain_password=SASL_PASSWORD,
)
