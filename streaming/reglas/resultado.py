from dataclasses import asdict, dataclass, field

GRAVEDAD_ALTA = "alta"
GRAVEDAD_MEDIA = "media"


@dataclass(frozen=True)
class ResultadoRegla:
    regla: str
    motivo: str
    gravedad: str
    detalle: dict = field(default_factory=dict)

    def como_dict(self) -> dict:
        return asdict(self)
