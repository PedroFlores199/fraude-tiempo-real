CREATE TABLE IF NOT EXISTS metricas (
    fecha TIMESTAMPTZ PRIMARY KEY,
    ventana_segundos DOUBLE PRECISION NOT NULL,
    transacciones INTEGER NOT NULL,
    transacciones_por_segundo DOUBLE PRECISION NOT NULL,
    porcentaje_bloqueadas DOUBLE PRECISION NOT NULL,
    porcentaje_revisadas DOUBLE PRECISION NOT NULL,
    latencia_media_ms DOUBLE PRECISION NOT NULL,
    latencia_max_ms DOUBLE PRECISION NOT NULL
);
