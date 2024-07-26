
DROP TABLE IF EXISTS public.h_covid_vacunados;

--/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

--creo tabla auxiliar de vacunados, con todos los  campos
CREATE TABLE IF NOT EXISTS public.h_covid_vacunados
(
    sexo VARCHAR(4) ,
    grupo_etario VARCHAR(5),
    jurisdiccion_residencia VARCHAR(19),
    jurisdiccion_residencia_id VARCHAR(2),
    depto_residencia VARCHAR(32),
    depto_residencia_id VARCHAR(3) ,
    jurisdiccion_aplicacion VARCHAR(19) ,
    jurisdiccion_aplicacion_id VARCHAR(2),
    depto_aplicacion VARCHAR(32) ,
    depto_aplicacion_id VARCHAR(3),
    fecha_aplicacion VARCHAR(10),
	vacuna VARCHAR(25),
	cod_dosis_generica VARCHAR(8),
    nombre_dosis_generica VARCHAR(16),
    condicion_aplicacion VARCHAR(40),
    orden_dosis VARCHAR(6),
    lote_vacuna VARCHAR(30),
	id_persona_dw VARCHAR(13)
);

--copio el csv de datos_nomivac_covid19
COPY h_covid_vacunados FROM 'B:/projects/covid/covid_forecast_framework/data/20231213/datos_nomivac_covid19.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF-8';

--/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

DROP TABLE IF EXISTS public.h_covid_casos;

--creo tabla casos auxiliar
CREATE TABLE IF NOT EXISTS public.h_covid_casos
(
	id_evento VARCHAR(8),
    sexo VARCHAR(2),
    edad VARCHAR(4),
    "edad_años_meses" VARCHAR(5),
    residencia_pais_nombre VARCHAR(32),
    residencia_provincia_nombre VARCHAR(19),
    residencia_departamento_nombre VARCHAR(29),
    carga_provincia_nombre VARCHAR(33),
    fecha_inicio_sintomas VARCHAR(10),
    fecha_apertura VARCHAR(10),
    sepi_apertura VARCHAR(2),
    fecha_internacion VARCHAR(10),
    cuidado_intensivo VARCHAR(2),
    fecha_cui_intensivo VARCHAR(10),
    fallecido VARCHAR(2),
    fecha_fallecimiento VARCHAR(10),
    asistencia_respiratoria_mecanica VARCHAR(2),
    carga_provincia_id VARCHAR(2),
    origen_fallecimiento VARCHAR(10),
    clasificacion VARCHAR(89),
    clasificacion_resumen VARCHAR(10),
    residencia_provincia_id VARCHAR(2),
    fecha_diagnostico VARCHAR(10),
    residencia_departamento_id VARCHAR(3),
    ultima_actualizacion VARCHAR(10)
);

--copio el csv Covid19Casos
COPY h_covid_casos FROM 'B:/projects/covid/covid_forecast_framework/data/20231213/Covid19Casos.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF-8';

--//////////////////////////////////////////////////////////////////////////////

DROP TABLE IF EXISTS public.conf_ambulatorios;

--creo tabla casos auxiliar
CREATE TABLE IF NOT EXISTS public.conf_ambulatorios
(
	IDEVENTOCASO VARCHAR(8),
    EVENTO VARCHAR(53),
    EDAD_DIAGNOSTICO VARCHAR(6),
    PAIS_RESIDENCIA VARCHAR(30),
	ID_PROV_INDEC_RESIDENCIA VARCHAR(2),
    PROVINCIA_RESIDENCIA VARCHAR(19),
    ID_DEPTO_INDEC_RESIDENCIA VARCHAR(5),
    DEPARTAMENTO_RESIDENCIA VARCHAR(30),
    FECHA_APERTURA VARCHAR(30),
    SEPI_APERTURA VARCHAR(2),
	FIS VARCHAR(30),
	ID_PROV_INDEC_CARGA VARCHAR(2),
	PROVINCIA_CARGA VARCHAR(25),
	CLASIFICACION_ALGORITMO VARCHAR(63)
);

--copio el csv de Confirmados_Ambulatorios
COPY conf_ambulatorios FROM 'B:/projects/covid/covid_forecast_framework/data/20231213/Confirmados_Ambulatorios.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF-8';


--/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


DROP TABLE IF EXISTS public.internaciones_y_fallecidos;

--creo tabla casos auxiliar
CREATE TABLE IF NOT EXISTS public.internaciones_y_fallecidos
(
	IDEVENTOCASO VARCHAR(8),
    EVENTO VARCHAR(53),
    EDAD_DIAGNOSTICO VARCHAR(6),
    PAIS_RESIDENCIA VARCHAR(30),
	ID_PROV_INDEC_RESIDENCIA VARCHAR(2),
    PROVINCIA_RESIDENCIA VARCHAR(32),
    ID_DEPTO_INDEC_RESIDENCIA VARCHAR(5),
    DEPARTAMENTO_RESIDENCIA VARCHAR(30),
    FECHA_APERTURA VARCHAR(30),
    SEPI_APERTURA VARCHAR(2),
	FIS VARCHAR(30),
	FECHA_INTERNACION VARCHAR(30),
	CUIDADO_INTENSIVO VARCHAR(2),
	FECHA_CUI_INTENSIVOS VARCHAR(30),
	FALLECIDO VARCHAR(2),
	FECHA_FALLECIMIENTO VARCHAR(30),
	ID_PROV_INDEC_CARGA VARCHAR(2),
	PROVINCIA_CARGA VARCHAR(25),
	CLASIFICACION_ALGORITMO VARCHAR(63)
);

--copio el csv de Covid19_Internados_y_Fallecidos
COPY internaciones_y_fallecidos FROM 'B:/projects/covid/covid_forecast_framework/data/20231213/Covid19_Internados_y_Fallecidos.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF-8';


--/////////////////////////////////////////////////////////////////////////////////////////