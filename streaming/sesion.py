from pyspark.sql import SparkSession

from comun.config import Config

PROVEEDOR_ESTADO = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"


def crear_sesion(config: Config, nombre: str = "deteccion-fraude") -> SparkSession:
    constructor = (
        SparkSession.builder.appName(nombre)
        .master(config.spark_master)
        .config("spark.sql.shuffle.partitions", config.spark_particiones)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.streaming.stateStore.providerClass", PROVEEDOR_ESTADO)
        .config("spark.sql.streaming.checkpointLocation", config.checkpoint_dir)
    )
    if config.spark_paquetes:
        constructor = constructor.config("spark.jars.packages", config.spark_paquetes)
    if config.spark_ivy:
        constructor = constructor.config("spark.jars.ivy", config.spark_ivy)
    return constructor.getOrCreate()
