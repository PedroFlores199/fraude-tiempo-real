import argparse
import logging
import random
from datetime import UTC, datetime, timedelta

from pymongo import MongoClient, ReplaceOne
from pymongo.collection import Collection

from comun.config import cargar_config
from comun.registro import configurar_logging
from generador.catalogo import (
    CATEGORIAS_HABITUALES,
    CIUDADES,
    COMERCIOS,
    GASTO_MEDIO,
)

logger = logging.getLogger(__name__)

NOMBRES = (
    "Lucía",
    "Hugo",
    "Martina",
    "Mateo",
    "Sofía",
    "Leo",
    "Julia",
    "Daniel",
    "Paula",
    "Pablo",
    "Valeria",
    "Álvaro",
    "Carmen",
    "Javier",
    "Elena",
    "Manuel",
    "Irene",
    "Sergio",
    "Marta",
    "Andrés",
    "Noelia",
    "Raúl",
    "Ainhoa",
    "Íñigo",
)

APELLIDOS = (
    "García",
    "Martínez",
    "López",
    "Sánchez",
    "González",
    "Rodríguez",
    "Fernández",
    "Pérez",
    "Gómez",
    "Martín",
    "Jiménez",
    "Ruiz",
    "Hernández",
    "Díaz",
    "Moreno",
    "Muñoz",
    "Álvarez",
    "Romero",
    "Navarro",
    "Torres",
)

OCUPACIONES = (
    "ingeniería",
    "medicina",
    "abogacía",
    "docencia",
    "enfermería",
    "programación",
    "diseño",
    "arquitectura",
    "comercio",
    "administración",
    "estudiante",
    "jubilación",
    "función pública",
    "investigación",
)

SECTORES = (
    "tecnología",
    "hostelería",
    "construcción",
    "logística",
    "comercio minorista",
    "servicios profesionales",
    "industria",
    "agricultura",
)

FORMAS_JURIDICAS = ("S.L.", "S.A.", "S.L.U.", "S. Coop.")

FECHA_REFERENCIA = datetime(2026, 1, 1, tzinfo=UTC)


def cliente_id(indice: int) -> str:
    return f"CLI-{indice:05d}"


def _tarjetas(rng: random.Random, tipo_cliente: str) -> list[dict]:
    if tipo_cliente == "empresa":
        cantidad = rng.randint(2, 5)
        tipos = ("credito", "empresa")
    else:
        cantidad = rng.choices((1, 2, 3), weights=(0.3, 0.45, 0.25))[0]
        tipos = ("debito", "credito")
    return [
        {
            "tarjeta_id": f"TAR-{rng.getrandbits(48):012x}",
            "tipo": rng.choice(tipos),
            "activa": True,
            "limite_mensual": float(rng.choice((1500, 3000, 6000, 12000))),
        }
        for _ in range(cantidad)
    ]


def _categorias(rng: random.Random, multiplicador: float) -> dict[str, dict]:
    elegidas = rng.sample(CATEGORIAS_HABITUALES, rng.randint(4, 7))
    resultado = {}
    for categoria in elegidas:
        minimo, maximo = GASTO_MEDIO[categoria]
        resultado[categoria] = {
            "gasto_medio": round(rng.uniform(minimo, maximo) * multiplicador, 2),
            "compras_mes": rng.randint(1, 25),
        }
    return resultado


def _comercios_frecuentes(rng: random.Random, categorias: dict[str, dict]) -> list[str]:
    frecuentes: list[str] = []
    for categoria in categorias:
        comercios = list(COMERCIOS[categoria])
        frecuentes.extend(rng.sample(comercios, rng.randint(1, min(3, len(comercios)))))
    return frecuentes


def _horario(rng: random.Random, tipo_cliente: str) -> dict[str, int]:
    if tipo_cliente == "empresa":
        return {"inicio": rng.randint(7, 9), "fin": rng.randint(18, 21)}
    return {"inicio": rng.randint(7, 10), "fin": rng.randint(20, 23)}


def generar_perfil(indice: int, semilla: int = 42) -> dict:
    rng = random.Random(f"{semilla}-{indice}")
    tipo_cliente = "empresa" if rng.random() < 0.12 else "particular"
    ciudad = rng.choice(list(CIUDADES))
    lat, lon = CIUDADES[ciudad]
    categorias = _categorias(rng, 3.0 if tipo_cliente == "empresa" else 1.0)
    perfil: dict = {
        "_id": cliente_id(indice),
        "tipo": tipo_cliente,
        "ciudad": ciudad,
        "ubicacion": {"lat": lat, "lon": lon},
        "tarjetas": _tarjetas(rng, tipo_cliente),
        "comercios_frecuentes": _comercios_frecuentes(rng, categorias),
        "categorias": categorias,
        "horario_tipico": _horario(rng, tipo_cliente),
        "fecha_alta": FECHA_REFERENCIA - timedelta(days=rng.randint(30, 3650)),
    }
    if tipo_cliente == "empresa":
        nombre = f"{rng.choice(APELLIDOS)} y {rng.choice(APELLIDOS)}"
        perfil["razon_social"] = f"{nombre} {rng.choice(FORMAS_JURIDICAS)}"
        perfil["cif"] = f"B{rng.randint(10_000_000, 99_999_999)}"
        perfil["sector"] = rng.choice(SECTORES)
        perfil["num_empleados"] = rng.randint(3, 250)
        perfil["persona_contacto"] = f"{rng.choice(NOMBRES)} {rng.choice(APELLIDOS)}"
    else:
        perfil["nombre"] = rng.choice(NOMBRES)
        perfil["apellidos"] = f"{rng.choice(APELLIDOS)} {rng.choice(APELLIDOS)}"
        perfil["edad"] = rng.randint(18, 85)
        perfil["ocupacion"] = rng.choice(OCUPACIONES)
    return perfil


def generar_perfiles(cantidad: int, semilla: int = 42, desde: int = 0) -> list[dict]:
    return [generar_perfil(indice, semilla) for indice in range(desde, desde + cantidad)]


def cargar_perfiles(coleccion: Collection, perfiles: list[dict]) -> int:
    if not perfiles:
        return 0
    operaciones = [ReplaceOne({"_id": p["_id"]}, p, upsert=True) for p in perfiles]
    resultado = coleccion.bulk_write(operaciones, ordered=False)
    return resultado.upserted_count + resultado.modified_count


def asegurar_poblacion(coleccion: Collection, cantidad: int, semilla: int = 42) -> int:
    existentes = coleccion.count_documents({})
    if existentes >= cantidad:
        return 0
    nuevos = generar_perfiles(cantidad - existentes, semilla, desde=existentes)
    return cargar_perfiles(coleccion, nuevos)


def main() -> None:
    config = cargar_config()
    configurar_logging(config.nivel_log)
    parser = argparse.ArgumentParser(description="Genera perfiles de clientes en MongoDB")
    parser.add_argument("--clientes", type=int, default=1000)
    parser.add_argument("--semilla", type=int, default=42)
    args = parser.parse_args()

    with MongoClient(config.mongo_uri, tz_aware=True) as cliente:
        coleccion = cliente[config.mongo_db]["clientes"]
        insertados = asegurar_poblacion(coleccion, args.clientes, args.semilla)
        total = coleccion.count_documents({})
    logger.info("Perfiles nuevos: %d. Total de clientes en MongoDB: %d", insertados, total)


if __name__ == "__main__":
    main()
