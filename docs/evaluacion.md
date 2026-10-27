# Evaluación de reglas y modelo

Generado con `python -m entrenamiento.evaluar` sobre un histórico sintético independiente del de entrenamiento: 86400 transacciones, 1297 fraudulentas. Parámetros: clientes=1000, dias=1.0, semilla=2026, umbral_revision=0.3, umbral_bloqueo=0.6.

## Comparativa

| estrategia | alertas | verdaderos_positivos | falsos_positivos | fraudes_no_detectados | precision | recall |
|---|---|---|---|---|---|---|
| solo_reglas | 796 | 740 | 56 | 557 | 0.930 | 0.571 |
| solo_modelo | 1452 | 909 | 543 | 388 | 0.626 | 0.701 |
| reglas_y_modelo | 1769 | 1171 | 598 | 126 | 0.662 | 0.903 |

## Recall por patrón de fraude

| patron | solo_reglas | solo_modelo | reglas_y_modelo | fraudes |
|---|---|---|---|---|
| importe_desproporcionado | 1.000 | 0.008 | 1.000 | 127 |
| madrugada_inusual | 1.000 | 0.556 | 1.000 | 9 |
| prueba_tarjeta | 0.458 | 0.861 | 0.877 | 1028 |
| viaje_imposible | 1.000 | 0.135 | 1.000 | 133 |
