CREATE DATABASE  sismos;


DROP TABLE IF EXISTS sismos;
CREATE TABLE sismos(
  id_sismo SERIAL PRIMARY KEY,
  fecha DATE,
  hora TIME,
  latitud DECIMAL,
  longitud DECIMAL,
  profundidad DECIMAL,
  magnitud DECIMAL,
  pais VARCHAR(255),
  estado VARCHAR(255),
  ciudad VARCHAR(255) NULL
);

DROP TABLE IF EXISTS lugar;
CREATE TABLE lugar(
  id_lugar SERIAL PRIMARY KEY,
  pais VARCHAR(255),
  estado VARCHAR(255)
);

DROP TABLE IF EXISTS tsunamis;
CREATE TABLE tsunamis(
  id_tsunami SERIAL PRIMARY KEY,
  fecha DATE,
  hora TIME,
  latitud DECIMAL,
  longitud DECIMAL,
  altura DECIMAL,
  distancia_inundacion DECIMAL
);
