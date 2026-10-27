from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.esquema_spark import motivo_error, struct_desde_esquema

ESQUEMA_DECISION = (
    "id_transaccion string, tarjeta_id string, cliente_id string, fecha_hora timestamp, "
    "decision string, motivos array<string>, puntuacion double, latencia_ms double, "
    "decidido_en timestamp"
)
FORMATO_ISO = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"


def parsear(kafka: DataFrame, esquema: dict) -> DataFrame:
    return (
        kafka.select(
            F.col("value").cast("string").alias("valor"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("topic").alias("topico"),
            F.col("partition").alias("particion"),
            F.col("offset"),
        )
        .withColumn("claves", F.json_object_keys("valor"))
        .withColumn("datos", F.from_json("valor", struct_desde_esquema(esquema)))
    )


def separar(parseado: DataFrame, esquema: dict) -> tuple[DataFrame, DataFrame]:
    con_motivo = parseado.withColumn("motivo_error", motivo_error(esquema))
    validas = (
        con_motivo.filter(F.col("motivo_error").isNull())
        .select("datos.*", "kafka_ts")
        .withColumn("fecha_evento", F.to_timestamp("fecha_hora"))
    )
    errores = con_motivo.filter(F.col("motivo_error").isNotNull()).select(
        "valor", "motivo_error", "kafka_ts", "topico", "particion", "offset"
    )
    return validas, errores


def mensajes_error(errores: DataFrame) -> DataFrame:
    return errores.select(
        F.lit(None).cast("string").alias("key"),
        F.to_json(
            F.struct(
                F.col("valor").alias("mensaje_original"),
                F.col("motivo_error").alias("motivo"),
                F.col("topico").alias("topico_origen"),
                F.col("particion"),
                F.col("offset"),
                F.date_format("kafka_ts", FORMATO_ISO).alias("recibido_en"),
            )
        ).alias("value"),
    )


def compras_por_ventana(
    validas: DataFrame,
    watermark: str,
    duracion: str = "1 minute",
    deslizamiento: str = "10 seconds",
    maximo: int = 5,
) -> DataFrame:
    return (
        validas.withWatermark("fecha_evento", watermark)
        .groupBy(F.window("fecha_evento", duracion, deslizamiento), "tarjeta_id", "cliente_id")
        .agg(
            F.count("*").alias("compras"),
            F.round(F.sum("importe"), 2).alias("importe_total"),
            F.collect_set("id_transaccion").alias("ids_transaccion"),
        )
        .filter(F.col("compras") > maximo)
        .select(
            F.col("window.start").alias("ventana_inicio"),
            F.col("window.end").alias("ventana_fin"),
            "tarjeta_id",
            "cliente_id",
            "compras",
            "importe_total",
            "ids_transaccion",
        )
    )


def mensajes_decision(decisiones: DataFrame) -> DataFrame:
    return decisiones.select(
        F.col("tarjeta_id").alias("key"),
        F.to_json(
            F.struct(
                "id_transaccion",
                "tarjeta_id",
                "cliente_id",
                "decision",
                "motivos",
                "puntuacion",
                "latencia_ms",
                F.date_format("fecha_hora", FORMATO_ISO).alias("fecha_hora"),
                F.date_format("decidido_en", FORMATO_ISO).alias("decidido_en"),
            )
        ).alias("value"),
    )


def resumen_lote(decisiones: DataFrame) -> DataFrame:
    return decisiones.agg(
        F.count("*").alias("transacciones"),
        F.coalesce(F.sum(F.when(F.col("decision") == "BLOQUEADA", 1)), F.lit(0)).alias(
            "bloqueadas"
        ),
        F.coalesce(F.sum(F.when(F.col("decision") == "REVISAR", 1)), F.lit(0)).alias("revisadas"),
        F.coalesce(F.sum("latencia_ms"), F.lit(0.0)).alias("suma_latencia_ms"),
        F.coalesce(F.max("latencia_ms"), F.lit(0.0)).alias("max_latencia_ms"),
    )
