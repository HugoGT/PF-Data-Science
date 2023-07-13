DROP TABLE IF EXISTS sismos;
CREATE TABLE sismos(
  id_sismo SERIAL PRIMARY KEY,
  fecha DATE,
  hora TIME,
  latitud DECIMAL,
  longitud DECIMAL,
  profundidad DECIMAL,
  magnitud DECIMAL,
  id_lugar INT
);

DROP TABLE IF EXISTS tsunamis;
CREATE TABLE tsunamis(
  id_tsunami SERIAL PRIMARY KEY,
  fecha DATE,
  hora TIME,
  altura_de_ola DECIMAL,
  id_sismo INT,
  id_lugar INT
);

DROP TABLE IF EXISTS danios;
CREATE TABLE danios(
  id_danio SERIAL PRIMARY KEY,
  fecha DATE,
  afectados INT,
  fallecidos INT,
  desaparecidos INT,
  heridos INT,
  viviendas_destruidas INT,
  viviendas_afectadas INT,
  id_sismo INT,
  id_lugar INT
);

DROP TABLE IF EXISTS lugares;
CREATE TABLE lugares(
  id_lugar SERIAL PRIMARY KEY,
  pais VARCHAR(50),
  estado VARCHAR(50)
);

ALTER TABLE sismos
ADD FOREIGN KEY (id_lugar) REFERENCES lugares(id_lugar);
ALTER TABLE tsunamis
ADD FOREIGN KEY (id_sismo) REFERENCES sismos(id_sismo),
ADD FOREIGN KEY (id_lugar) REFERENCES lugares(id_lugar);
ALTER TABLE danios
ADD FOREIGN KEY (id_sismo) REFERENCES sismos(id_sismo),
ADD FOREIGN KEY (id_lugar) REFERENCES lugares(id_lugar);
