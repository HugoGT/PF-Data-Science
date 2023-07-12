DROP TABLE IF EXISTS sismos;
CREATE TABLE sismos(
  id_sismo SERIAL PRIMARY KEY,
  fecha DATE,
  hora TIME,
  profundidad DECIMAL,
  magnitud DECIMAL,
  id_lugar INT,
  id_tsunami INT,
  id_danio INT
);

DROP TABLE IF EXISTS lugar;
CREATE TABLE lugar(
  id_lugar SERIAL PRIMARY KEY,
  ciudad VARCHAR(50),
  estado VARCHAR(50),
  latitud DECIMAL,
  longitud DECIMAL
);

DROP TABLE IF EXISTS tsunamis;
CREATE TABLE tsunamis(
  id_tsunami SERIAL PRIMARY KEY,
  fecha DATE,
  hora TIME,
  max_water_height DECIMAL,
  max_inundation_distance DECIMAL
);

DROP TABLE IF EXISTS danios;
CREATE TABLE danios(
  id_danios SERIAL PRIMARY KEY,
  fecha DATE,
  damnificados INT,
  afectados INT,
  viviendas_destruidas INT,
  viviendas_afectadas INT
);

ALTER TABLE sismos
ADD FOREIGN KEY (id_lugar) REFERENCES lugar(id_lugar),
ADD FOREIGN KEY (id_tsunami) REFERENCES tsunamis(id_tsunami),
ADD FOREIGN KEY (id_danio) REFERENCES danios(id_danios);
