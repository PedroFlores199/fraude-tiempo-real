from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T

TIPOS_SPARK = {
    "string": T.StringType(),
    "number": T.DoubleType(),
    "integer": T.LongType(),
    "boolean": T.BooleanType(),
}


def struct_desde_esquema(esquema: dict) -> T.StructType:
    requeridos = set(esquema.get("required", []))
    return T.StructType(
        [
            T.StructField(nombre, TIPOS_SPARK[definicion["type"]], nombre not in requeridos)
            for nombre, definicion in esquema["properties"].items()
        ]
    )


def _condiciones_campo(nombre: str, definicion: dict, claves: Column) -> list[tuple[Column, str]]:
    campo = F.col(f"datos.{nombre}")
    presente = campo.isNotNull()
    condiciones = [(F.array_contains(claves, nombre) & campo.isNull(), f"tipo_invalido:{nombre}")]
    if "enum" in definicion:
        condiciones.append(
            (presente & ~campo.isin(definicion["enum"]), f"valor_no_permitido:{nombre}")
        )
    if "pattern" in definicion:
        condiciones.append(
            (presente & ~campo.rlike(definicion["pattern"]), f"formato_invalido:{nombre}")
        )
    if "minimum" in definicion:
        condiciones.append((presente & (campo < definicion["minimum"]), f"fuera_de_rango:{nombre}"))
    if "exclusiveMinimum" in definicion:
        condiciones.append(
            (presente & (campo <= definicion["exclusiveMinimum"]), f"fuera_de_rango:{nombre}")
        )
    if "maximum" in definicion:
        condiciones.append((presente & (campo > definicion["maximum"]), f"fuera_de_rango:{nombre}"))
    if "minLength" in definicion:
        condiciones.append(
            (presente & (F.length(campo) < definicion["minLength"]), f"longitud_invalida:{nombre}")
        )
    if "maxLength" in definicion:
        condiciones.append(
            (presente & (F.length(campo) > definicion["maxLength"]), f"longitud_invalida:{nombre}")
        )
    if definicion.get("format") == "date-time":
        condiciones.append((presente & F.to_timestamp(campo).isNull(), f"fecha_invalida:{nombre}"))
    return condiciones


def motivo_error(esquema: dict, claves: str = "claves") -> Column:
    columna_claves = F.col(claves)
    propiedades = esquema["properties"]
    condiciones: list[tuple[Column, str]] = [(columna_claves.isNull(), "json_mal_formado")]
    if esquema.get("additionalProperties") is False:
        permitidas = F.array(*[F.lit(nombre) for nombre in propiedades])
        condiciones.append(
            (F.size(F.array_except(columna_claves, permitidas)) > 0, "campos_no_permitidos")
        )
    for nombre in esquema.get("required", []):
        condiciones.append((~F.array_contains(columna_claves, nombre), f"falta_campo:{nombre}"))
    for nombre, definicion in propiedades.items():
        condiciones.extend(_condiciones_campo(nombre, definicion, columna_claves))

    expresion = F.lit(None).cast("string")
    for condicion, motivo in reversed(condiciones):
        expresion = F.when(condicion, F.lit(motivo)).otherwise(expresion)
    return expresion
