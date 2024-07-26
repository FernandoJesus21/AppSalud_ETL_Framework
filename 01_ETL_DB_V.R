

##############################################################################################################
# Script de ETL para la tabla h_covid_vacunados
# Este script se encarga de procesar el conjunto de los datos de aplicaciones de vacunas
##############################################################################################################

inicio0 <- Sys.time()
options(scipen=6) #para evitar notacion cientifica
path0  <-  getwd()
script_version <- "3.0.0"
fecha_script_version <- "13-01-2024"

##############################################################################################################
zz0 <- file(paste(path0, "/salida/registro/01_ETL_DB_V_", gsub("[:| |-]", "_", inicio0), ".txt", sep = ""), open="wt")
sink(zz0, type = "message")
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


#Comienzo de mensajes
message("01_ETL_DB_V: \n")
message("Version: ", script_version, " | Fecha version: ", fecha_script_version)

message("###################################################\n")
message("PARAMETROS: \n")
message("INICIO: ", inicio0, "\n")
message("PATH: ", path0, "\n")
message("###################################################\n")



#creacion de la listas que guardaran los resultados de cada extracción de BBDD
lista_tramos_h_covid_vacunados <- list()

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
# APLICACIONES DE VACUNAS, Extraccion de datos
# Voy leyendo la BBDD en tramos de un mes recuperando datos especificos en cada modulo ETL
###########################################################################################

message("===================================================\n")
message("INICIO del proceso de lectura a tramos: \n")
message("===================================================\n")


#Comienzo la consulta maestra en la tabla h_covid_vacunados
i_t <- Sys.time()
for (i in 2020:fin) {
  for (j in 1:12) {
    
    #compatibilizo el contador para usarlos en la fecha: si es menor a 10 le agrego un 0 adelante
    aux_i <- as.character(i)
    aux_j <- ifelse(j < 10, paste0("0", as.character(j)), as.character(j))
    
    #consulta para leer el tramo de datos
    h_covid_vacunados <- dbGetQuery(con, paste0("SELECT grupo_etario AS rango_etario, 
                                                  fecha_aplicacion AS date, 
                                                  jurisdiccion_residencia AS provincia, 
                                                  depto_residencia AS departamento,
                                                  vacuna, 
                                                  nombre_dosis_generica AS tipo_dosis
                                           FROM h_covid_vacunados 
                                           WHERE fecha_aplicacion LIKE '",aux_i, "-", aux_j,"%'; "))
    
    #mensage indicando la cantidad de registros leidos
    message("Tramo [",aux_i, aux_j,"]: ", nrow(h_covid_vacunados), " registros.\n")
    #si el tramo leido tiene al menos un registro
    if(nrow(h_covid_vacunados) != 0){
      #adapto fechas al formato Date
      h_covid_vacunados$date <- as.Date(h_covid_vacunados$date)
      #Correccion de datos: corrijo provincias y departamentos
      h_covid_vacunados <- h_covid_vacunados %>%
        corregirPcias_SI() %>%
        corregirDeptos_SI() %>%
        corregirRangoEtario_SI()
      #agrego en la lista el tramo agrupado
      lista_tramos_h_covid_vacunados[[paste0(aux_i, aux_j)]] <- h_covid_vacunados %>%
        group_by(rango_etario,
                 date, 
                 provincia, 
                 departamento, 
                 vacuna, 
                 tipo_dosis) %>%
        summarise(vacunas_aplicadas = n(),
                  .groups = "drop")
    }
    
    #tiempo de espera predefinido
    Sys.sleep(0.1)
    #elimino el tramo que ya se proceso
    rm(h_covid_vacunados)
    #llamo al garbage collector
    gc()
    #tiempo de espera predefinido
    Sys.sleep(0.25)
  }
}
#mido el tiempo consumido en la lectura y preproceso del tramo
f_t <- Sys.time()
message("transcurrido en ", (f_t - i_t) ," \n")



#########################################################################################
# Creación del dataframe central (DFC)
#  a partir de la lista de tramos armo el dataframe central
#########################################################################################

message("===================================================\n")
message("INICIO del proceso de generación del dataframe central (DFC): \n")
message("===================================================\n")


#Colapso de lista a df
DFC_preeliminar <- lista_tramos_h_covid_vacunados %>%
  bind_rows() 

#guardo la fecha del ultimo registro
etl_info <- data.frame("V_data_last_update" = max(DFC_preeliminar$date))

#proceso de correccion de datos
DFC_preeliminar <- corregirCasosAmbiguos(DFC_preeliminar)

#hago el resto de creacion de variables y correcciones al DFC
DFC <- left_join(indice_general, DFC_preeliminar, by = c("date", "provincia" , "departamento"), multiple = "all") %>% #junta del DFC con el indice general
  mutate(rango_etario = ifelse(is.na(rango_etario), "*na*", rango_etario)) %>% #si no hay dato de rango etario pongo la etiqueta *na*
  mutate(vacunas_aplicadas = ifelse(is.na(vacunas_aplicadas), 0, vacunas_aplicadas)) %>% #si no hay dato de aplicaciones pongo 0
  mutate(semana_aplicacion =  1 + as.numeric(date - as.Date("2020-12-27")) %/% 7) %>% #genero la variable semana
  mutate(mes_aplicacion = paste0(year(date), ifelse(nchar(month(date)) == 1, paste0("0", month(date)), month(date)))) %>% #genero la variable mes de aplicacion
  mutate(departamento = ifelse(is.na(departamento), "*na*", departamento)) %>% #si no hay dato de departamento pongo la etiqueta *na*
  mutate(departamento = ifelse(departamento == "*na*", "*na*", departamento)) %>% #si no hay dato de departamento pongo la etiqueta *na*
  mutate(provincia = ifelse(provincia == "*na*", "*na*", provincia)) %>% #si no hay dato de departamento pongo la etiqueta *na*
  mutate(tipo_dosis = ifelse(is.na(tipo_dosis), "*na*", tipo_dosis)) %>% #si no hay nombre de dosis pongo la etiqueta *na*
  filter(semana_aplicacion > 0) %>% #elimino registros con semana < 0 indicando que su fecha fue mal cargada
  mutate(vacuna = ifelse(is.na(vacuna), "*na*", vacuna)) #si no hay dato de rango etario pongo la etiqueta *na*


#guardo la fecha del ultimo registro despues de hacer el join con el calendario maestro
# de esta manera obtengo la fecha de realizacion del ETL
etl_info$etl_date <- max(DFC$date)

#exporto el dataframe con la fecha del ultimo dato original
write.fst(etl_info, "salida/fst/reload_date.fst")

#exportando el dataframe central
message("Exportando DFC_V.fst\n")
write.fst(DFC, "salida/fst/DFC_V.fst")

#desconexion de la base datos
dbDisconnect(con)
#limpiando
message("--- Limpiando ---\n")
rm(lista_tramos_h_covid_vacunados, aux_i, aux_j, DFC_preeliminar)
#llamo al garbage collector
gc()


#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
fin0 <- Sys.time()
TIEMPO_EJECUCION <- fin0 - inicio0
message("\n# FIN de ejecucion 01_ETL_DB_V.R en ", round(TIEMPO_EJECUCION, 2) ,"\n###################################################\n")
sink(type="message")
##############################################################################################################

close(zz0)

#limpiando
cat("--- Limpiando ---\n")
#elimino objeto innecesarios
#rm(aux_Re)
#llamo al garbage collector
gc()
cat("Fin de ejecución de 01_ETL_DB_V.R\n")


