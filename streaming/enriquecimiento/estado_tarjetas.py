from collections import defaultdict
from datetime import datetime

from comun.features import EstadoTarjeta
from comun.tiempo import a_epoch_ms, desde_epoch_ms, desde_iso

MINUTO_MS = 60_000
DIA_MS = 86_400_000


def clave_historial(tarjeta_id: str) -> str:
    return f"tarjeta:{tarjeta_id}:historial"


def clave_rafaga(tarjeta_id: str) -> str:
    return f"tarjeta:{tarjeta_id}:rafaga"


def _miembro(transaccion: dict) -> str:
    return "|".join(
        (
            transaccion["id_transaccion"],
            f"{float(transaccion['importe']):.2f}",
            f"{float(transaccion['lat']):.6f}",
            f"{float(transaccion['lon']):.6f}",
        )
    )


def resumir_historial(
    miembros, id_transaccion: str, ts: int, rafaga_detectada: bool = False
) -> EstadoTarjeta:
    compras_minuto = 0
    gasto = 0.0
    anterior: tuple[float, str, float, float] | None = None
    for miembro, puntuacion in miembros:
        texto = miembro.decode("utf-8") if isinstance(miembro, bytes) else miembro
        identificador, importe, lat, lon = texto.split("|")
        if identificador == id_transaccion:
            continue
        gasto += float(importe)
        if puntuacion >= ts - MINUTO_MS:
            compras_minuto += 1
        if anterior is None or (puntuacion, texto) > anterior[:2]:
            anterior = (puntuacion, texto, float(lat), float(lon))
    return EstadoTarjeta(
        ultima_lat=anterior[2] if anterior else None,
        ultima_lon=anterior[3] if anterior else None,
        ultima_fecha=desde_epoch_ms(int(anterior[0])) if anterior else None,
        compras_ultimo_minuto=compras_minuto,
        gasto_24h=round(gasto, 2),
        rafaga_detectada=rafaga_detectada,
    )


class EstadoTarjetasRedis:
    def __init__(self, cliente, ttl_historial_s: int, ttl_rafaga_s: int) -> None:
        self._cliente = cliente
        self._ttl_historial_s = ttl_historial_s
        self._ttl_rafaga_s = ttl_rafaga_s

    def leer(self, tarjeta_id: str, fecha: datetime, id_transaccion: str) -> EstadoTarjeta:
        ts = a_epoch_ms(fecha)
        tuberia = self._cliente.pipeline(transaction=False)
        tuberia.zrangebyscore(clave_historial(tarjeta_id), ts - DIA_MS, ts, withscores=True)
        tuberia.exists(clave_rafaga(tarjeta_id))
        historial, rafaga = tuberia.execute()
        return resumir_historial(historial, id_transaccion, ts, bool(rafaga))

    def registrar(self, transaccion: dict) -> None:
        clave = clave_historial(transaccion["tarjeta_id"])
        ts = a_epoch_ms(desde_iso(transaccion["fecha_hora"]))
        tuberia = self._cliente.pipeline(transaction=False)
        tuberia.zadd(clave, {_miembro(transaccion): ts})
        tuberia.zremrangebyscore(clave, "-inf", f"({ts - DIA_MS}")
        tuberia.expire(clave, self._ttl_historial_s)
        tuberia.execute()

    def marcar_rafaga(self, tarjeta_id: str) -> None:
        self._cliente.set(clave_rafaga(tarjeta_id), 1, ex=self._ttl_rafaga_s)


class EstadoTarjetasMemoria:
    def __init__(self) -> None:
        self._historial: dict[str, dict[str, tuple[int, str]]] = defaultdict(dict)

    def leer(self, tarjeta_id: str, fecha: datetime, id_transaccion: str) -> EstadoTarjeta:
        ts = a_epoch_ms(fecha)
        miembros = [
            (miembro, puntuacion)
            for puntuacion, miembro in self._historial[tarjeta_id].values()
            if ts - DIA_MS <= puntuacion <= ts
        ]
        return resumir_historial(miembros, id_transaccion, ts)

    def registrar(self, transaccion: dict) -> None:
        historial = self._historial[transaccion["tarjeta_id"]]
        ts = a_epoch_ms(desde_iso(transaccion["fecha_hora"]))
        historial[transaccion["id_transaccion"]] = (ts, _miembro(transaccion))
        caducados = [k for k, (puntuacion, _) in historial.items() if puntuacion < ts - DIA_MS]
        for identificador in caducados:
            del historial[identificador]
