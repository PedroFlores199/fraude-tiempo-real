const fs = require("fs");

const directorioEsquemas = process.env.ESQUEMAS_DIR || "/scripts/esquemas";
const base = db.getSiblingDB(process.env.MONGO_DB || "fraude");

function leerEsquema(nombre) {
  return JSON.parse(fs.readFileSync(`${directorioEsquemas}/${nombre}.json`, "utf8"));
}

function aplicarValidador(coleccion, esquema) {
  const opciones = {
    validator: { $jsonSchema: esquema },
    validationLevel: "strict",
    validationAction: "error",
  };
  if (base.getCollectionNames().includes(coleccion)) {
    base.runCommand({ collMod: coleccion, ...opciones });
  } else {
    base.createCollection(coleccion, opciones);
  }
}

aplicarValidador("clientes", leerEsquema("clientes"));
aplicarValidador("alertas", leerEsquema("alertas"));

base.clientes.createIndex({ ciudad: 1 }, { name: "ciudad" });
base.clientes.createIndex({ tipo: 1 }, { name: "tipo" });
base.clientes.createIndex({ "tarjetas.tarjeta_id": 1 }, { name: "tarjeta", unique: true });

base.alertas.createIndex({ estado: 1, fecha: -1 }, { name: "estado_fecha" });
base.alertas.createIndex({ cliente_id: 1, fecha: -1 }, { name: "cliente_fecha" });
base.alertas.createIndex({ motivos: 1, fecha: -1 }, { name: "motivo_fecha" });
base.alertas.createIndex(
  { descartada_en: 1 },
  { name: "ttl_descartadas_30_dias", expireAfterSeconds: 30 * 24 * 3600 }
);

base.etiquetas.createIndex({ fecha: 1 }, { name: "ttl_etiquetas_7_dias", expireAfterSeconds: 7 * 24 * 3600 });
base.rafagas.createIndex({ tarjeta_id: 1, ventana_inicio: -1 }, { name: "tarjeta_ventana" });
base.rafagas.createIndex(
  { ventana_inicio: 1 },
  { name: "ttl_rafagas_7_dias", expireAfterSeconds: 7 * 24 * 3600 }
);

print(`MongoDB preparado en la base ${base.getName()}: validadores e índices aplicados`);
