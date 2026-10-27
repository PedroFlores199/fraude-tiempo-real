from datetime import UTC, datetime
from zoneinfo import ZoneInfo

ZONA_LOCAL = ZoneInfo("Europe/Madrid")


def ahora_utc() -> datetime:
    return datetime.now(UTC)


def a_utc(valor: datetime) -> datetime:
    if valor.tzinfo is None:
        return valor.replace(tzinfo=UTC)
    return valor.astimezone(UTC)


def a_iso(valor: datetime) -> str:
    return a_utc(valor).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def desde_iso(texto: str) -> datetime:
    return a_utc(datetime.fromisoformat(texto.replace("Z", "+00:00")))


def a_epoch_ms(valor: datetime) -> int:
    return int(a_utc(valor).timestamp() * 1000)


def desde_epoch_ms(milisegundos: int) -> datetime:
    return datetime.fromtimestamp(milisegundos / 1000, tz=UTC)


def hora_local(valor: datetime) -> int:
    return a_utc(valor).astimezone(ZONA_LOCAL).hour


def dia_semana_local(valor: datetime) -> int:
    return a_utc(valor).astimezone(ZONA_LOCAL).weekday()
