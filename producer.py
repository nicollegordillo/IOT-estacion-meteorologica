"""Productor IoT: simula una estación meteorológica y publica en Kafka.

Uso:
    python producer.py              # JSON al topic 22246, cada 15-30 s
    python producer.py --compact    # 3 bytes al topic 22246-compact
    python producer.py --min 2 --max 3   # intervalo corto solo para pruebas

Se detiene con Ctrl+C.
"""

import argparse
import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

import config
from codec import DIRECCIONES, encode

MEDIA = 55
DESVIACION = 15  # ~99.7 % de los valores cae entre 10 y 100


def generar_data() -> dict:
    """Lee los tres 'sensores' simulados."""
    temperatura = round(min(max(random.gauss(MEDIA, DESVIACION), 0), 110), 2)
    humedad = int(round(min(max(random.gauss(MEDIA, DESVIACION), 0), 100)))
    direccion = random.choice(DIRECCIONES)
    return {
        "temperatura": temperatura,
        "humedad": humedad,
        "direccion_viento": direccion,
    }


def main():
    parser = argparse.ArgumentParser(description="Productor IoT - Estación meteorológica")
    parser.add_argument("--compact", action="store_true",
                        help="Enviar payload de 3 bytes (parte 3.4)")
    parser.add_argument("--min", type=float, default=15, help="Intervalo mínimo (s)")
    parser.add_argument("--max", type=float, default=30, help="Intervalo máximo (s)")
    args = parser.parse_args()

    topic = config.TOPIC_COMPACT if args.compact else config.TOPIC_JSON
    modo = "3 bytes" if args.compact else "JSON"

    producer = KafkaProducer(**config.KAFKA_AUTH, acks="all")
    print(f"Conectado a {config.BOOTSTRAP_SERVERS} | topic='{topic}' | modo={modo}")
    print("Ctrl+C para detener.\n")

    enviados = 0
    try:
        while True:
            data = generar_data()

            if args.compact:
                value = encode(data)
                mostrado = value.hex(" ").upper()
            else:
                value = json.dumps(data).encode("utf-8")
                mostrado = value.decode("utf-8")

            meta = producer.send(
                topic, key=config.KEY.encode("utf-8"), value=value
            ).get(timeout=10)
            enviados += 1

            hora = datetime.now().strftime("%H:%M:%S")
            print(f"[{hora}] #{enviados} {data}")
            print(f"           -> {len(value)} bytes: {mostrado} "
                  f"(partición {meta.partition}, offset {meta.offset})")

            time.sleep(random.uniform(args.min, args.max))
    except KeyboardInterrupt:
        print(f"\nDetenido. Mensajes enviados: {enviados}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
