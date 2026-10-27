import os
from dataclasses import dataclass
from functools import lru_cache


def _texto(nombre: str, defecto: str) -> str:
    return os.environ.get(nombre, defecto)


def _entero(nombre: str, defecto: int) -> int:
    return int(os.environ.get(nombre, defecto))


def _decimal(nombre: str, defecto: float) -> float:
    return float(os.environ.get(nombre, defecto))


@dataclass(frozen=True)
class Config:
    kafka_servidores: str
    topico_transacciones: str
    topico_errores: str
    topico_decisiones: str
    mongo_uri: str
    mongo_db: str
    redis_host: str
    redis_puerto: int
    postgres_dsn: str
    modelo_ruta: str
    checkpoint_dir: str
    spark_master: str
    spark_paquetes: str
    spark_ivy: str
    kafka_offsets_iniciales: str
    spark_particiones: int
    watermark: str
    trigger_segundos: int
    tps: int
    tasa_fraude: float
    tasa_retraso: float
    intervalo_minimo_tarjeta_s: int
    umbral_revision: float
    umbral_bloqueo: float
    perfil_cache_ttl_s: int
    ttl_historial_s: int
    ttl_rafaga_s: int
    intervalo_metricas_s: int
    nivel_log: str


@lru_cache(maxsize=1)
def cargar_config() -> Config:
    return Config(
        kafka_servidores=_texto("KAFKA_SERVIDORES", "localhost:9092"),
        topico_transacciones=_texto("TOPICO_TRANSACCIONES", "transacciones"),
        topico_errores=_texto("TOPICO_ERRORES", "errores"),
        topico_decisiones=_texto("TOPICO_DECISIONES", "decisiones"),
        mongo_uri=_texto("MONGO_URI", "mongodb://localhost:27017/?directConnection=true"),
        mongo_db=_texto("MONGO_DB", "fraude"),
        redis_host=_texto("REDIS_HOST", "localhost"),
        redis_puerto=_entero("REDIS_PUERTO", 6379),
        postgres_dsn=_texto("POSTGRES_DSN", "postgresql://fraude:fraude@localhost:5433/fraude"),
        modelo_ruta=_texto("MODELO_RUTA", "modelos/modelo.joblib"),
        checkpoint_dir=_texto("CHECKPOINT_DIR", "checkpoints"),
        spark_master=_texto("SPARK_MASTER", "local[*]"),
        spark_paquetes=_texto("SPARK_PAQUETES", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"),
        spark_ivy=_texto("SPARK_IVY", ""),
        kafka_offsets_iniciales=_texto("KAFKA_OFFSETS_INICIALES", "latest"),
        spark_particiones=_entero("SPARK_PARTICIONES", 6),
        watermark=_texto("WATERMARK", "2 minutes"),
        trigger_segundos=_entero("TRIGGER_SEGUNDOS", 1),
        tps=_entero("TPS", 100),
        tasa_fraude=_decimal("TASA_FRAUDE", 0.015),
        tasa_retraso=_decimal("TASA_RETRASO", 0.03),
        intervalo_minimo_tarjeta_s=_entero("INTERVALO_MINIMO_TARJETA_S", 15),
        umbral_revision=_decimal("UMBRAL_REVISION", 0.3),
        umbral_bloqueo=_decimal("UMBRAL_BLOQUEO", 0.6),
        perfil_cache_ttl_s=_entero("PERFIL_CACHE_TTL_S", 300),
        ttl_historial_s=_entero("TTL_HISTORIAL_S", 90000),
        ttl_rafaga_s=_entero("TTL_RAFAGA_S", 600),
        intervalo_metricas_s=_entero("INTERVALO_METRICAS_S", 10),
        nivel_log=_texto("NIVEL_LOG", "INFO"),
    )
