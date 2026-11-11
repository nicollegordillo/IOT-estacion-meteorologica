import argparse
import json
from collections import Counter
from datetime import datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from kafka import KafkaConsumer

import config
from codec import DIRECCIONES, decode


def procesar_mensaje(valor: bytes, compact: bool) -> dict:
    if compact:
        return decode(valor)
    return json.loads(valor.decode("utf-8"))


def crear_figura():
    plt.ion()
    fig, (ax_t, ax_h, ax_w) = plt.subplots(3, 1, figsize=(10, 9))
    fig.subplots_adjust(hspace=0.55)
    return fig, ax_t, ax_h, ax_w


def graficar(fig, ejes, tiempos, temps, humes, vientos, titulo):
    ax_t, ax_h, ax_w = ejes
    formato_hora = mdates.DateFormatter("%H:%M:%S")

    ax_t.clear()
    ax_t.plot(tiempos, temps, marker="o", color="tab:red")
    ax_t.set_title("Temperatura")
    ax_t.set_ylabel("°C")
    ax_t.set_ylim(0, 110)
    ax_t.xaxis.set_major_formatter(formato_hora)
    ax_t.tick_params(axis="x", rotation=30)
    ax_t.grid(alpha=0.3)

    ax_h.clear()
    ax_h.plot(tiempos, humes, marker="o", color="tab:blue")
    ax_h.set_title("Humedad relativa")
    ax_h.set_ylabel("%")
    ax_h.set_ylim(0, 100)
    ax_h.xaxis.set_major_formatter(formato_hora)
    ax_h.tick_params(axis="x", rotation=30)
    ax_h.grid(alpha=0.3)

    ax_w.clear()
    conteo = Counter(vientos)
    valores = [conteo.get(d, 0) for d in DIRECCIONES]
    ax_w.bar(DIRECCIONES, valores, color="tab:green")
    ax_w.set_title("Dirección del viento (conteo)")
    ax_w.set_ylabel("Mensajes")
    ax_w.set_ylim(0, max(valores + [1]) + 1)
    ax_w.yaxis.get_major_locator().set_params(integer=True)

    fig.suptitle(f"{titulo} | mensajes: {len(tiempos)}", fontsize=13)
    fig.canvas.draw_idle()
    fig.canvas.flush_events()


def main():
    parser = argparse.ArgumentParser(description="Consumidor IoT - Estación meteorológica")
    parser.add_argument("--compact", action="store_true",
                        help="Leer payload de 3 bytes (parte 3.4)")
    parser.add_argument("--group", default=config.GROUP_ID,
                        help="group_id")
    args = parser.parse_args()

    topic = config.TOPIC_COMPACT if args.compact else config.TOPIC_JSON
    modo = "3 bytes" if args.compact else "JSON"

    consumer = KafkaConsumer(
        topic,
        group_id=args.group,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        **config.KAFKA_AUTH,
    )
    print(f"Conectado a {config.BOOTSTRAP_SERVERS} | topic='{topic}' | "
          f"group_id='{args.group}' | modo={modo}")
    print("Esperando mensajes... (Ctrl+C o cerrar la ventana para detener)\n")

    tiempos, temps, humes, vientos = [], [], [], []
    fig, *ejes = crear_figura()
    titulo = f"Estación meteorológica - topic {topic} ({modo})"
    graficar(fig, ejes, tiempos, temps, humes, vientos, titulo)

    recibidos = 0
    try:
        while plt.fignum_exists(fig.number):
            lotes = consumer.poll(timeout_ms=1000)
            hubo_nuevos = False

            for mensajes in lotes.values():
                for msg in mensajes:
                    try:
                        payload = procesar_mensaje(msg.value, args.compact)
                        t = float(payload["temperatura"])
                        h = int(payload["humedad"])
                        w = payload["direccion_viento"]
                    except (ValueError, KeyError, TypeError, UnicodeDecodeError) as e:
                        print(f"[omitido] offset {msg.offset}: mensaje inválido ({e})")
                        continue

                    tiempos.append(datetime.fromtimestamp(msg.timestamp / 1000))
                    temps.append(t)
                    humes.append(h)
                    vientos.append(w)
                    recibidos += 1
                    hubo_nuevos = True

                    print(f"#{recibidos} (partición {msg.partition}, offset {msg.offset}) "
                        f"{len(msg.value)} bytes -> {payload}")

            if hubo_nuevos:
                graficar(fig, ejes, tiempos, temps, humes, vientos, titulo)
            plt.pause(0.1)
    except KeyboardInterrupt:
        pass
    finally:
        print(f"\nDetenido. Mensajes procesados: {recibidos}")
        if recibidos and plt.fignum_exists(fig.number):
            archivo = "grafica_compact.png" if args.compact else "grafica_json.png"
            fig.savefig(archivo, dpi=200, bbox_inches="tight")
            print(f"Captura guardada en {archivo}")
        consumer.close()
        plt.close("all")


if __name__ == "__main__":
    main()