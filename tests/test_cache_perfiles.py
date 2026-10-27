from streaming.enriquecimiento.perfiles import CachePerfiles


class Reloj:
    def __init__(self) -> None:
        self.ahora = 0.0

    def __call__(self) -> float:
        return self.ahora


def test_busca_por_id_y_omite_desconocidos(mongo, perfil):
    mongo["clientes"].insert_one(perfil)
    cache = CachePerfiles(mongo["clientes"], ttl_s=60)
    encontrados = cache.obtener_varios(["CLI-00001", "CLI-00404"])
    assert list(encontrados) == ["CLI-00001"]
    assert "fecha_alta" not in encontrados["CLI-00001"]
    assert cache.obtener("CLI-00404") is None


def test_refresca_los_perfiles_caducados(mongo, perfil):
    mongo["clientes"].insert_one(perfil)
    reloj = Reloj()
    cache = CachePerfiles(mongo["clientes"], ttl_s=60, reloj=reloj)
    assert cache.obtener("CLI-00001")["ciudad"] == "Madrid"
    mongo["clientes"].update_one({"_id": "CLI-00001"}, {"$set": {"ciudad": "Sevilla"}})
    reloj.ahora = 30
    assert cache.obtener("CLI-00001")["ciudad"] == "Madrid"
    reloj.ahora = 61
    assert cache.obtener("CLI-00001")["ciudad"] == "Sevilla"


def test_precarga_toda_la_coleccion(mongo, perfil):
    mongo["clientes"].insert_many([perfil, {**perfil, "_id": "CLI-00002"}])
    cache = CachePerfiles(mongo["clientes"], ttl_s=60)
    assert cache.precargar() == 2
    mongo["clientes"].delete_many({})
    assert cache.obtener("CLI-00002") is not None
