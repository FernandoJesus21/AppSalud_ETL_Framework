
##############################################################################################################
# Script de ETL para la tabla h_covid_casos
# Este script se encarga de procesar el conjunto de los casos confirmados fallecidos
##############################################################################################################

inicio0 <- Sys.time()
options(scipen=6) #para evitar notacion cientifica
path0  <-  getwd()
script_version <- "3.0.0"
fecha_script_version <- "13-01-2024"

##############################################################################################################
zz0 <- file(paste(path0, "/salida/registro/01_ETL_DB_CF_", gsub("[:| |-]", "_", inicio0), ".txt", sep = ""), open="wt")
sink(zz0, type = "message")
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


#Comienzo de mensajes
message("01_ETL_DB_CF: \n")
message("Version: ", script_version, " | Fecha version: ", fecha_script_version)

message("###################################################\n")
message("PARAMETROS: \n")
message("INICIO: ", inicio0, "\n")
message("PATH: ", path0, "\n")
message("###################################################\n")

#creacion de la listas que guardaran los resultados de cada extracción de BBDD
lista_tramos_h_covid_casos <- list()

#definiendo cotas de tiempo
fin <- year(Sys.Date())

#TC para establecer la conexion a la BBDD
tryCatch(expr = {
  message("Intentando acceder a la fuente de datos: \n")
  con <- dbConnect(RPostgres::Postgres(), dbname = DB_NAME, host=HOST_DB, port=DB_PORT, user=DB_USER, password=DB_PASS) 
  message("Conexion exitosa: \n")
  message("Accediendo a las tablas: ", dbListTables(con), "\n")
},
error = function(e){
  message("\n------------------------------------------------------------------------------------------\n")
  message("Se encontraron errores en la carga de datos.\n")
  message(e)
  stop("\n")
},
warning = function(e){
  message("\n------------------------------------------------------------------------------------------\n")
  message("Se encontraron warnings en la carga de datos.\n")
  message(e)
}
)


###########################################################################################
# CASOS CONFIRMADOS FALLECIDOS COVID, Extraccion de datos
# Voy leyendo la BBDD en tramos de un mes recuperando datos especificos en cada modulo ETL
###########################################################################################

message("===================================================\n")
message("INICIO del proceso de lectura a tramos: \n")
message("===================================================\n")

#ETL h_covid_casos serie confirmados fallecidos (historico hasta el 4/6/22)

i_t <- Sys.time()
for (i in 2020:fin) {
  for (j in 1:12) {
    
    #compatibilizo el contador para usarlos en la fecha: si es menor a 10 le agrego un 0 adelante
    aux_i <- as.character(i)
    aux_j <- ifelse(j < 10, paste0("0", as.character(j)), as.character(j))
    
    #consulta para leer el tramo de datos
    tramo_h_casos_casos <- dbGetQuery(con, paste0("SELECT 
                                            fecha_fallecimiento AS date, 
                                            edad,
                                            residencia_provincia_nombre AS provincia, 
                                            residencia_departamento_nombre AS departamento  
                                          FROM h_covid_casos 
                                          WHERE clasificacion_resumen = 'Confirmado' 
                                          AND fallecido = 'SI' 
                                          AND fecha_fallecimiento LIKE '",aux_i, "-", aux_j,"%'; ")) 
    
    #mensage indicando la cantidad de registros leidos
    message("Tramo [",aux_i, aux_j,"]: ", nrow(tramo_h_casos_casos), " registros.\n")
    #si el tramo leido tiene al menos un registro
    if(nrow(tramo_h_casos_casos) != 0){
      #adapto fechas al formato Date
      tramo_h_casos_casos$date <- as.Date(tramo_h_casos_casos$date)
      #Correccion de datos: corrijo provincias y departamentos
      tramo_h_casos_casos <- tramo_h_casos_casos %>%
        corregirPcias_SI() %>%
        corregirDeptos_SI()
      #agrego en la lista el tramo agrupado
      lista_tramos_h_covid_casos[[paste0(aux_i, aux_j)]] <- tramo_h_casos_casos %>%
        group_by(date,
                 edad,
                 provincia, 
                 departamento) %>%
        summarise(fallecidos = n(),
                  .groups = "drop")
    }
    
    #tiempo de espera predefinido
    Sys.sleep(0.1)
    #elimino el tramo que ya se proceso
    rm(tramo_h_casos_casos)
    #llamo al garbage collector
    gc()
    #tiempo de espera predefinido
    Sys.sleep(0.25)
  }
}
#mido el tiempo consumido en la lectura y preproceso del tramo
f_t <- Sys.time()
message("transcurrido en ", (f_t - i_t) ," \n")


#ETL h_covid_casos serie fallecidos (del 5/6/22 hasta la actualidad)
# NO es necesario realizar el proceso de lectura a tramos por la poca cantidad de registros

i_t <- Sys.time()
#consulta para leer los registros al completo
internaciones_y_fallecimientos <- dbGetQuery(con, "SELECT 
                            fecha_fallecimiento AS date,
                            edad_diagnostico AS edad, 
                            provincia_residencia AS provincia,
                            departamento_residencia AS departamento
                          FROM internaciones_y_fallecidos 
                          WHERE (clasificacion_algoritmo = 'SARS-COV-2 por test de antígeno' 
                          OR clasificacion_algoritmo = 'SARS-COV-2 por métodos moleculares')
                          AND fallecido = 'SI';
                          ") 
#si el dataframe leido tiene al menos un registro
if(nrow(internaciones_y_fallecimientos) != 0){
  #adapto fechas al formato Date
  internaciones_y_fallecimientos$date <- substr(internaciones_y_fallecimientos$date, 1, 10)
  internaciones_y_fallecimientos$date <- as.Date(internaciones_y_fallecimientos$date)
  #Correccion de datos: corrijo provincias y departamentos
  internaciones_y_fallecimientos <- internaciones_y_fallecimientos %>%
    corregirPcias_SI() %>%
    corregirDeptos_SI()
  #agrupo el dataframe
  internaciones_y_fallecimientos <- internaciones_y_fallecimientos %>%
    group_by(date, 
             edad,
             provincia, 
             departamento) %>%
    summarise(fallecidos = n(),
              .groups = "drop")
}
#tiempo de espera predefinido
Sys.sleep(0.1)
f_t <- Sys.time()
#mido el tiempo consumido en la lectura y preproceso del tramo
message("transcurrido en ", (f_t - i_t) ," \n")

#########################################################################################
# Creación del dataframe central (DFC)
#  a partir de la lista de tramos armo el dataframe central
#########################################################################################


message("===================================================\n")
message("INICIO del proceso de generación del dataframe central (DFC): \n")
message("===================================================\n")


#junto los dataset historico con el reciente
DFC_preeliminar <- lista_tramos_h_covid_casos %>%
  bind_rows %>%
  rbind(internaciones_y_fallecimientos) 

#proceso de correccion de datos
DFC_preeliminar <- corregirCasosAmbiguos(DFC_preeliminar)

#hago el resto de creacion de variables y correcciones al DFC
DFC <- left_join(indice_general, DFC_preeliminar, by = c("date", "provincia" , "departamento"), multiple = "all") %>% #junta del DFC con el indice general
  mutate(edad = ifelse(is.na(edad), "*na*", edad)) %>% #si no hay dato de edad pongo la etiqueta *na*
  mutate(fallecidos = ifelse(is.na(fallecidos), 0, fallecidos)) %>% #si no hay dato de fallecidos pongo 0
  mutate(semana =  1 + as.numeric(date - as.Date("2020-03-01")) %/% 7) %>% #genero la variable semana
  mutate(mes = paste0(year(date), ifelse(nchar(month(date)) == 1, paste0("0", month(date)), month(date)))) %>% #genero la variable mes
  mutate(departamento = ifelse(is.na(departamento), "*na*", departamento)) %>% #si no hay dato de departamento pongo la etiqueta *na*
  mutate(departamento = ifelse(departamento == "*sin dato*", "*na*", departamento)) %>% #si no hay dato de departamento pongo la etiqueta *na*
  mutate(provincia = ifelse(provincia == "*sin dato*", "*na*", provincia)) %>% #si no hay dato de departamento pongo la etiqueta *na*
  filter(semana > 0) #elimino registros con semana < 0 indicando que su fecha fue mal cargada

#exportando el dataframe central
message("Exportando DFC_CF.fst\n")
write.fst(DFC, "salida/fst/DFC_CF.fst")

#desconexion de la base datos
dbDisconnect(con)
#limpiando
message("--- Limpiando ---\n")
rm(internaciones_y_fallecimientos, aux_i, aux_j, lista_tramos_h_covid_casos)
#llamo al garbage collector
gc()

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
fin0 <- Sys.time()
TIEMPO_EJECUCION <- fin0 - inicio0
message("\n# Fin de ejecución de 01_ETL_DB_CF.R en ", round(TIEMPO_EJECUCION, 2) ,"\n###################################################\n")
sink(type="message")
##############################################################################################################

close(zz0)

#limpiando
cat("--- Limpiando ---\n")
#elimino objeto innecesarios
rm(aux_Re, DFC_CF)
#llamo al garbage collector
gc()
cat("Fin de ejecución de 01_ETL_DB_CF.R\n")

