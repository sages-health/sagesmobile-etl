CREATE SEQUENCE oevisit_sequence
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 2286
  CACHE 1;
ALTER TABLE oevisit_sequence
  OWNER TO postgres;

CREATE TABLE oevisit
(
  id integer NOT NULL DEFAULT nextval('oevisit_sequence'::regclass),
  sex text,
  age integer,
  CONSTRAINT id PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE oevisit
  OWNER TO postgres;
