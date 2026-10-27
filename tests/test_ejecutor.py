from datetime import UTC, datetime

from streaming.ejecutor import documento_rafaga


def test_documento_de_rafaga_convierte_la_hora_local_de_spark_a_utc():
    inicio = datetime(2026, 10, 27, 10, 0, tzinfo=UTC)
    fin = datetime(2026, 10, 27, 10, 1, tzinfo=UTC)
    fila = {
        "ventana_inicio": inicio.astimezone().replace(tzinfo=None),
        "ventana_fin": fin.astimezone().replace(tzinfo=None),
        "tarjeta_id": "TAR-0000000000aa",
        "cliente_id": "CLI-00001",
        "compras": 7,
        "importe_total": 10.5,
        "ids_transaccion": ["b", "a"],
    }
    documento = documento_rafaga(fila)
    assert documento["_id"] == "TAR-0000000000aa|2026-10-27T10:00:00.000Z"
    assert documento["ventana_inicio"] == inicio
    assert documento["ventana_fin"] == fin
    assert documento["ids_transaccion"] == ["a", "b"]
