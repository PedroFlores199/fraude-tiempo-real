CIUDADES: dict[str, tuple[float, float]] = {
    "Madrid": (40.4168, -3.7038),
    "Barcelona": (41.3874, 2.1686),
    "Valencia": (39.4699, -0.3763),
    "Sevilla": (37.3891, -5.9845),
    "Bilbao": (43.2630, -2.9350),
    "Málaga": (36.7213, -4.4217),
    "Zaragoza": (41.6488, -0.8891),
    "Murcia": (37.9922, -1.1307),
    "A Coruña": (43.3623, -8.4115),
    "Valladolid": (41.6523, -4.7245),
    "Palma": (39.5696, 2.6502),
    "Granada": (37.1773, -3.5986),
}

COMERCIOS: dict[str, dict[str, str]] = {
    "alimentacion": {
        "Mercadona": "presencial",
        "Carrefour": "presencial",
        "Lidl": "presencial",
        "Dia": "presencial",
        "Eroski": "presencial",
        "Alcampo": "presencial",
    },
    "restauracion": {
        "Telepizza": "presencial",
        "Goiko": "presencial",
        "100 Montaditos": "presencial",
        "VIPS": "presencial",
        "Starbucks": "presencial",
        "Glovo": "online",
    },
    "transporte": {
        "Renfe": "online",
        "Cabify": "online",
        "Repsol": "presencial",
        "Cepsa": "presencial",
        "BlaBlaCar": "online",
        "Metro de Madrid": "presencial",
    },
    "electronica": {
        "MediaMarkt": "presencial",
        "PcComponentes": "online",
        "Fnac": "presencial",
        "Apple Store": "presencial",
        "Worten": "presencial",
    },
    "moda": {
        "Zara": "presencial",
        "Mango": "presencial",
        "Primark": "presencial",
        "Bershka": "presencial",
        "Massimo Dutti": "presencial",
        "Zalando": "online",
    },
    "ocio": {
        "Cines Yelmo": "presencial",
        "Ticketmaster": "online",
        "Netflix": "online",
        "Spotify": "online",
        "Steam": "online",
    },
    "salud": {
        "Farmacia Central": "presencial",
        "Vitaldent": "presencial",
        "Óptica Universitaria": "presencial",
        "Sanitas": "online",
    },
    "hogar": {
        "Ikea": "presencial",
        "Leroy Merlin": "presencial",
        "Bricomart": "presencial",
        "Conforama": "presencial",
    },
    "viajes": {
        "Iberia": "online",
        "Vueling": "online",
        "Booking.com": "online",
        "Airbnb": "online",
        "Meliá": "online",
    },
    "suministros": {
        "Iberdrola": "online",
        "Naturgy": "online",
        "Endesa": "online",
        "Movistar": "online",
        "Vodafone": "online",
    },
    "joyeria": {
        "Tous": "presencial",
        "Pandora": "presencial",
        "Joyería Suárez": "presencial",
        "Swarovski": "presencial",
    },
    "apuestas": {
        "Bet365": "online",
        "Codere": "online",
        "Sportium": "online",
        "PokerStars": "online",
    },
}

GASTO_MEDIO: dict[str, tuple[float, float]] = {
    "alimentacion": (20.0, 90.0),
    "restauracion": (10.0, 45.0),
    "transporte": (8.0, 60.0),
    "electronica": (60.0, 400.0),
    "moda": (25.0, 120.0),
    "ocio": (10.0, 50.0),
    "salud": (10.0, 80.0),
    "hogar": (30.0, 200.0),
    "viajes": (80.0, 400.0),
    "suministros": (30.0, 120.0),
    "joyeria": (80.0, 500.0),
    "apuestas": (10.0, 100.0),
}

CATEGORIAS_HABITUALES: tuple[str, ...] = (
    "alimentacion",
    "restauracion",
    "transporte",
    "moda",
    "ocio",
    "salud",
    "hogar",
    "suministros",
    "electronica",
    "viajes",
)

CATEGORIAS_RARAS: tuple[str, ...] = ("joyeria", "apuestas", "electronica", "viajes")

CATEGORIA_DE_COMERCIO: dict[str, str] = {
    comercio: categoria for categoria, comercios in COMERCIOS.items() for comercio in comercios
}

CANAL_DE_COMERCIO: dict[str, str] = {
    comercio: canal for comercios in COMERCIOS.values() for comercio, canal in comercios.items()
}
