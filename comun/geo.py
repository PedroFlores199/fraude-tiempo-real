from math import atan2, cos, radians, sin, sqrt

RADIO_TIERRA_KM = 6371.0


def distancia_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    delta_lat = radians(lat2 - lat1)
    delta_lon = radians(lon2 - lon1)
    a = sin(delta_lat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(delta_lon / 2) ** 2
    return RADIO_TIERRA_KM * 2 * atan2(sqrt(a), sqrt(1 - a))
