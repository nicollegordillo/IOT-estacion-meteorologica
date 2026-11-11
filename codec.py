"""Codificación compartida del payload de 3 bytes (24 bits).

Layout (big-endian, bit 23 = más significativo):
    bits 23-10 (14 bits): temperatura * 100   -> 0..11000  (2^14 = 16384)
    bits  9-3  ( 7 bits): humedad              -> 0..100    (2^7  = 128)
    bits  2-0  ( 3 bits): índice de dirección  -> 0..7      (2^3  = 8)
"""

DIRECCIONES = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]


def encode(data: dict) -> bytes:
    """JSON (dict) -> 3 bytes."""
    t = round(data["temperatura"] * 100)
    h = int(data["humedad"])
    w = DIRECCIONES.index(data["direccion_viento"])

    if not 0 <= t <= 11000:
        raise ValueError(f"Temperatura fuera de rango: {data['temperatura']}")
    if not 0 <= h <= 100:
        raise ValueError(f"Humedad fuera de rango: {h}")

    v = (t << 10) | (h << 3) | w
    return v.to_bytes(3, "big")


def decode(payload: bytes) -> dict:
    """3 bytes -> JSON (dict)."""
    if len(payload) != 3:
        raise ValueError(f"Se esperaban 3 bytes, llegaron {len(payload)}")
    v = int.from_bytes(payload, "big")
    return {
        "temperatura": ((v >> 10) & 0x3FFF) / 100,
        "humedad": (v >> 3) & 0x7F,
        "direccion_viento": DIRECCIONES[v & 0x7],
    }


if __name__ == "__main__":
    # Prueba rápida: python codec.py
    ejemplos = [
        {"temperatura": 56.32, "humedad": 51, "direccion_viento": "SO"},
        {"temperatura": 110.0, "humedad": 100, "direccion_viento": "NE"},
        {"temperatura": 0.0, "humedad": 0, "direccion_viento": "N"},
    ]
    for d in ejemplos:
        b = encode(d)
        print(f"{d} -> {b.hex(' ').upper()} -> {decode(b)}")
        assert decode(b) == d
    print("OK: encode/decode coinciden")
