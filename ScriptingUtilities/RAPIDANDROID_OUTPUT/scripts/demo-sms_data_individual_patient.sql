-- Table: sms_data_individual_patient

-- DROP TABLE sms_data_individual_patient;

CREATE TABLE sms_data_individual_patient
(
  _id character varying,
  id integer NOT NULL DEFAULT nextval('data_reports_id_seq'::regclass),
  uuid character varying,
  report_date timestamp without time zone,
  district character varying NOT NULL,
  sex character varying NOT NULL,
  age integer,
  patient_id character varying,
  return_visit boolean,
  weight double precision,
  temperature double precision,
  pulse integer,
  bp_systolic double precision,
  bp_diastolic double precision,
  report_symptoms character varying,
  report_diagnoses character varying,
  notes character varying,
  user_id integer DEFAULT 3,
  phonenumber character varying,
  sms_received timestamp without time zone,
  create_date timestamp without time zone,
  modified_date timestamp without time zone
)
WITH (
  OIDS=FALSE
);
ALTER TABLE sms_data_individual_patient
  OWNER TO postgres;