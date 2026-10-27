import json
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator

RUTA_ESQUEMA = Path(__file__).resolve().parent.parent / "esquema" / "transaccion.json"


@lru_cache(maxsize=1)
def cargar_esquema() -> dict:
    return json.loads(RUTA_ESQUEMA.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def validador() -> Draft202012Validator:
    return Draft202012Validator(
        cargar_esquema(), format_checker=Draft202012Validator.FORMAT_CHECKER
    )


def errores_validacion(transaccion: dict) -> list[str]:
    return sorted(
        f"{'/'.join(str(parte) for parte in error.path) or 'mensaje'}: {error.message}"
        for error in validador().iter_errors(transaccion)
    )


def es_valida(transaccion: dict) -> bool:
    return validador().is_valid(transaccion)


def categorias() -> list[str]:
    return list(cargar_esquema()["properties"]["categoria"]["enum"])
