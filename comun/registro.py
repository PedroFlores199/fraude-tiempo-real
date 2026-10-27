import logging

FORMATO = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def configurar_logging(nivel: str = "INFO") -> None:
    logging.basicConfig(level=nivel.upper(), format=FORMATO)
