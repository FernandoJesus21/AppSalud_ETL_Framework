
##############################################################################################################
# Script de definicion de constantes
# Este script es necesario ejecutarse antes de la etapa 2 de ejecucion

#indice_general: calendario maestro por dia, provincia y departamento
#provincias: provincias con su identificador y dato de poblacion

##############################################################################################################


#importando indice de provincias con su dato de poblacion
provincias <- read_csv("data/provincias.csv",
                       col_types = cols(idp = col_character(),
                                        provincia = col_character()))


#importando dataset con los detalles de cada departamento
lista_departamentos_api <- read_delim("data/indice_pcias_deptos_completo.csv", 
                                      delim = ";", escape_double = FALSE, col_types = cols(centroide_lat = col_character(), 
                                                                                           centroide_lon = col_character()), 
                                      trim_ws = TRUE)

#genero un nuevo dataframe quitando columnas innecesarias de la lista de departamentos, cambio nombres y tipos de variables
indice_pcias_deptos <- copy(lista_departamentos_api) %>%
  select(id, residencia_provincia_id, residencia_provincia_nombre, residencia_departamento_id, residencia_departamento_nombre) %>%
  setnames(old = c("id", "residencia_provincia_id", "residencia_provincia_nombre", "residencia_departamento_id", "residencia_departamento_nombre"),
           new = c("idg", "idp", "provincia", "idd", "departamento")) %>%
  mutate(idp = as.character(idp)) %>%
  mutate(idg = as.character(idg))


#creacion de un calendario maestro desde enero 2020 hasta la fecha de ejecucion del script
calendario <- seq(as.Date("2020-01-01"), Sys.Date(), "days")

#agrego al calendario maestro las variables de provincias y departamentos
indice_general <- crossing(calendario, indice_pcias_deptos) %>%
  setnames(old = "calendario", new = "date")


#elimino elementos innecesarios
rm(calendario, indice_pcias_deptos, lista_departamentos_api)

