
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
zz0 <- file(paste(path0, "/salida/registro/02_ETL_V_", gsub("[:| |-]", "_", inicio0), ".txt", sep = ""), open="wt")
sink(zz0, type = "message")
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


#Comienzo de mensajes
message("02_ETL_V: \n")
message("Version: ", script_version, " | Fecha version: ", fecha_script_version)

message("###################################################\n")
message("PARAMETROS: \n")
message("INICIO: ", inicio0, "\n")
message("PATH: ", path0, "\n")
message("###################################################\n")


#mensaje por log para indicar la lectura a traves de un DFC existente
if(usar_DFC_existente){
  message("Establecida opción de lectura a partir de DFC existente...\n")
}

#TC para establecer la conexion a la BBDD
tryCatch(expr = {
  message("Importando DFC_V.fst\n")
  DFC <- read.fst("salida/fst/DFC_V.fst", as.data.table = T)
  
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
message("Apertura exitosa del archivo.\n")




message("===================================================\n")
message("INICIO del proceso de generación de los dataframe secundarios (DFS): \n")
message("===================================================\n")

#Aplicaciones de vacunas agrupado por dia y provincia
V_GDP <- copy(DFC) %>%
  group_by(date, idp, provincia) %>%
  summarise(valor = sum(vacunas_aplicadas),
            .groups = "drop") %>%
  #setnames(old = c("fecha_aplicacion", "jurisdiccion_residencia", "vacunas_aplicadas"), new = c("date", "provincia", "valor")) %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "D") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]")

message("Generado: V_GDP\n")

#Aplicaciones de vacunas acumulado agrupado por dia y provincia
V_AC_GDP <- copy(V_GDP) %>%
  group_by(idp, provincia, c1, c2, idg, departamento) %>%
  arrange(date) %>%
  mutate(valor = cumsum(valor)) %>%
  mutate(c1 = "V_AC")

message("Generado: V_AC_GDP\n")

#Aplicaciones de vacunas agrupado por semana y provincia
V_GSP <- copy(DFC) %>%
  group_by(semana_aplicacion, idp, provincia) %>%
  summarise(date = min(date),
            valor = sum(vacunas_aplicadas),
            .groups = "drop") %>%
  setnames(old = c("semana_aplicacion"), new = c("semana")) %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "S") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-semana)

message("Generado: V_GSP\n")

#Aplicaciones de vacunas agrupado por mes y provincia
V_GMP <- copy(DFC) %>%
  group_by(mes_aplicacion, idp, provincia) %>%
  summarise(date = max(date),
            valor = sum(vacunas_aplicadas),
            .groups = "drop") %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "M") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-mes_aplicacion)

message("Generado: V_GMP\n")

#Aplicaciones de vacunas agrupado por dia
V_GD <- copy(DFC) %>%
  group_by(date) %>%
  summarise(valor = sum(vacunas_aplicadas),
            .groups = "drop") %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "D") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]")

message("Generado: V_GD\n")

#Aplicaciones de vacunas acumulado agrupado por dia
V_AC_GD <- copy(V_GD) %>%
  group_by(idp, provincia, c1, c2, idg, departamento) %>%
  arrange(date) %>%
  mutate(valor = cumsum(valor)) %>%
  mutate(c1 = "V_AC")

message("Generado: V_AC_GD\n")

#Aplicaciones de vacunas agrupado por semana
V_GS <- copy(DFC) %>%
  group_by(semana_aplicacion) %>%
  summarise(date = min(date),
            valor = sum(vacunas_aplicadas),
            .groups = "drop") %>%
  setnames(old = c("semana_aplicacion"), new = c("semana")) %>%
  mutate(c1 = "V") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  mutate(c2 = "S") %>%
  select(-semana)

message("Generado: V_GS\n")

#Aplicaciones de vacunas agrupado por mes
V_GM <- copy(DFC) %>%
  group_by(mes_aplicacion) %>%
  summarise(date = max(date),
            valor = sum(vacunas_aplicadas),
            .groups = "drop") %>%
  setnames(old = c("mes_aplicacion"), new = c("mes")) %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "M") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-mes)

message("Generado: V_GM\n")

#Aplicaciones de vacunas agrupado por mes, provincia y vacuna
V_GMPV <- copy(DFC) %>%
  group_by(mes_aplicacion, idp, provincia, vacuna) %>%
  summarise(date = max(date),
            valor = sum(vacunas_aplicadas),
            .groups = "drop") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "M")

message("Generado: V_GMPV\n")


#Aplicaciones de vacunas agrupado por mes, provincia y departamento
V_GMPDe <- copy(DFC) %>%
  group_by(mes_aplicacion, idp, provincia, idg, departamento) %>%
  summarise(date = max(date),
            valor = sum(vacunas_aplicadas),
            .groups = "drop") %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "M") %>%
  select(-mes_aplicacion)

message("Generado: V_GMPDe\n")

#Aplicaciones de vacunas agrupado por año, provincia y departamento
V_GAPDe <- copy(DFC) %>%
  group_by(year(date), idp, provincia, idg, departamento) %>%
  summarise(date = max(date),
            valor = sum(vacunas_aplicadas),
            .groups = "drop") %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "A") %>%
  setnames(old = c("year(date)"), new = c("a_o")) %>%
  select(-a_o)

message("Generado: V_GAPDe\n")

#Aplicaciones de vacunas agrupado por año
V_GA <- copy(DFC) %>%
  group_by(year(date)) %>%
  summarise(date = max(date),
            valor = sum(vacunas_aplicadas),
            .groups = "drop") %>%
  setnames(old = c("year(date)"), new = c("a_o")) %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "A") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-a_o)

message("Generado: V_GA\n")

#Aplicaciones de vacunas agrupado por año y provincia
V_GAP <- copy(DFC) %>%
  group_by(year(date), idp, provincia) %>%
  summarise(date = max(date),
            valor = sum(vacunas_aplicadas),
            .groups = "drop") %>%
  setnames(old = c("year(date)"), new = c("a_o")) %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "A") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-a_o)

message("Generado: V_GAP\n")


#Aplicaciones de vacunas agrupado por mes, provincia y rango etario
V_GMPRe <- copy(DFC) %>%
  mutate(periodo = paste0(year(date), month(date))) %>%
  group_by(periodo, idp, provincia, rango_etario) %>%
  summarise(valor = sum(vacunas_aplicadas),
            date = max(date),
            .groups = "drop") %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "M") %>%
  select(-periodo)

message("Generado: V_GMPRe\n")

#Aplicaciones de vacunas agrupado por año, provincia y rango etario
V_GAPRe <- copy(DFC) %>%
  group_by(year(date), idp, provincia, rango_etario) %>%
  summarise(valor = sum(vacunas_aplicadas),
            date = max(date),
            .groups = "drop") %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "A") %>%
  setnames(old = "year(date)", new = "a_o") %>%
  select(-a_o)

message("Generado: V_GAPRe\n")

#Aplicaciones de vacunas agrupado por mes y rango etario
V_GMRe <- copy(DFC) %>%
  mutate(periodo = paste0(year(date), month(date))) %>%
  group_by(periodo, rango_etario) %>%
  summarise(valor = sum(vacunas_aplicadas),
            date = max(date),
            .groups = "drop") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "M") %>%
  select(-periodo)

message("Generado: V_GMRe\n")

#Aplicaciones de vacunas agrupado por año y rango etario
V_GARe <- copy(DFC) %>%
  group_by(year(date),  rango_etario) %>%
  summarise(valor = sum(vacunas_aplicadas),
            date = max(date),
            .groups = "drop") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "A") %>%
  setnames(old = "year(date)", new = "a_o") %>%
  select(-a_o)

message("Generado: V_GARe\n")

#Aplicaciones de vacunas agrupado por mes, provincia y tipo de dosis
V_GMPCdg <- copy(DFC) %>%
  mutate(periodo = paste0(year(date), ifelse(nchar(month(date)) == 1, paste0("0", month(date)), month(date)))) %>%
  group_by(periodo, idp, provincia, tipo_dosis) %>%
  summarise(
    valor = sum(vacunas_aplicadas),
    .groups = "drop") %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "M") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  mutate(tipo_dosis = ifelse(is.na(tipo_dosis), "*na*", tipo_dosis))

V_GMPCdg <- as.data.table(V_GMPCdg) %>%
  data.table::dcast(periodo + idp + provincia + c1 + c2 + idg + departamento ~ tipo_dosis, value.var = "valor") %>%
  select(-c("*na*"))

V_GMPCdg[is.na(V_GMPCdg)] <- 0
V_GMPCdg$VesqC <- V_GMPCdg$`2da` + V_GMPCdg$Unica #variable cantidad personas con esquema completo de vacunacion

V_GMPCdg <- V_GMPCdg %>%
  mutate(date = ymd(paste0(substr(periodo, 1,4), "-", substr(periodo, 5,6), "-01"))) %>%
  mutate(date = date + months(1)) %>%
  mutate(date = date - days(1))

message("Generado: V_GMPCdg\n")

V_GMPCdg2 <- as.data.table(select(V_GMPCdg, -c("c1", "periodo"))) %>%
  melt(id.vars = c("idp", "provincia", "idg", "departamento", "c2", "date"),
       measure.vars = c("1ra", "2da", "Adicional", "Refuerzo", "Unica", "VesqC")) %>%
  setnames(old = c("variable", "value"), new = c("c1", "valor"))

#Aplicaciones de vacunas agrupado por mes, provincia, departamento y tipo de dosis
V_GMPDeCdg <- copy(DFC) %>%
  mutate(periodo = paste0(year(date), ifelse(nchar(month(date)) == 1, paste0("0", month(date)), month(date)))) %>%
  group_by(periodo, idp, provincia, idg, departamento, tipo_dosis) %>%
  summarise(
    valor = sum(vacunas_aplicadas),
    .groups = "drop") %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "M") %>%
  mutate(tipo_dosis = ifelse(is.na(tipo_dosis), "*na*", tipo_dosis))

V_GMPDeCdg <- as.data.table(V_GMPDeCdg) %>%
  data.table::dcast(periodo + idp + provincia + c1 + c2 + idg + departamento ~ tipo_dosis, value.var = "valor") %>%
  select(-c("*na*"))

V_GMPDeCdg[is.na(V_GMPDeCdg)] <- 0
V_GMPDeCdg$VesqC <- V_GMPDeCdg$`2da` + V_GMPDeCdg$Unica #variable cantidad personas con esquema completo de vacunacion

V_GMPDeCdg <- V_GMPDeCdg %>%
  mutate(date = ymd(paste0(substr(periodo, 1,4), "-", substr(periodo, 5,6), "-01"))) %>%
  mutate(date = date + months(1)) %>%
  mutate(date = date - days(1))

message("Generado: V_GMPDeCdg\n")

V_GMPDeCdg2 <- as.data.table(select(V_GMPDeCdg, -c("c1", "periodo"))) %>%
  melt(id.vars = c("idp", "provincia", "idg", "departamento", "c2", "date"),
       measure.vars = c("1ra", "2da", "Adicional", "Refuerzo", "Unica", "VesqC")) %>%
  setnames(old = c("variable", "value"), new = c("c1", "valor"))




#VACUNADOS SEGUN PROVINCIA, MES Y ORDEN DOSIS (otro formato)

V_GMPCdg3 <- as.data.table(select(V_GMPCdg, -c("c1", "periodo", "VesqC"))) %>%
  melt(id.vars = c("idp", "provincia", "idg", "departamento", "c2", "date"),
       measure.vars = c("1ra", "2da", "Adicional", "Refuerzo", "Unica")) %>%
  setnames(old = c("variable", "value"), new = c("c1", "valor"))

#Aplicaciones de vacunas agrupado por año, provincia y tipo de dosis
V_GAPCdg <- copy(DFC) %>%
  mutate(a_o = year(date)) %>%
  group_by(a_o, idp, provincia, tipo_dosis) %>%
  summarise(
    valor = sum(vacunas_aplicadas),
    .groups = "drop") %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "A") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  mutate(tipo_dosis = ifelse(is.na(tipo_dosis), "*na*", tipo_dosis))

V_GAPCdg <- as.data.table(V_GAPCdg) %>%
  data.table::dcast(a_o +  idp + provincia + c1 + c2 + idg + departamento ~ tipo_dosis, value.var = "valor") %>%
  select(-c("*na*"))

V_GAPCdg[is.na(V_GAPCdg)] <- 0
V_GAPCdg$VesqC <- V_GAPCdg$`2da` + V_GAPCdg$Unica #variable cantidad personas con esquema completo de vacunacion


V_GAPCdg <- V_GAPCdg %>%
  mutate(date = ymd(paste0(a_o, "-12-31")))

message("Generado: V_GAPCdg\n")

V_GAPCdg2 <- as.data.table(select(V_GAPCdg, -c("c1", "a_o"))) %>%
  melt(id.vars = c("idp", "provincia", "idg", "departamento", "c2", "date"),
       measure.vars = c("1ra", "2da", "Adicional", "Refuerzo", "Unica", "VesqC")) %>%
  setnames(old = c("variable", "value"), new = c("c1", "valor"))


#VACUNADOS SEGUN MES Y ORDEN DOSIS (otro formato, para mapa de calor)
V_GMCdg3 <- copy(DFC) %>%
  group_by(mes_aplicacion, tipo_dosis) %>%
  summarise(
    valor = sum(vacunas_aplicadas),
    .groups = "drop"
  ) %>%
  mutate(tipo_dosis = ifelse(is.na(tipo_dosis), "*na*", tipo_dosis)) %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "M") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]")

V_GMCdg3 <- as.data.table(V_GMCdg3) %>%
  data.table::dcast(mes_aplicacion + idp + provincia + c1 + c2 + idg + departamento ~ tipo_dosis, value.var = "valor") %>%
  select(-c("*na*")) 

V_GMCdg3[is.na(V_GMCdg3)] <- 0

V_GMCdg3 <- V_GMCdg3 %>%
  mutate(date = ymd(paste0(substr(mes_aplicacion, 1,4), "-", substr(mes_aplicacion, 5,6), "-01"))) %>%
  mutate(date = date + months(1)) %>%
  mutate(date = date - days(1))

V_GMCdg3 <- as.data.table(select(V_GMCdg3, -c("c1"))) %>%
  melt(id.vars = c("idp", "provincia", "idg", "departamento", "c2", "date"),
       measure.vars = c("1ra", "2da", "Adicional", "Refuerzo", "Unica")) %>%
  setnames(old = c("variable", "value"), new = c("c1", "valor"))

#Aplicaciones de vacunas agrupado por mes y tipo de dosis
V_GMCdg <- copy(DFC) %>%
  mutate(periodo = paste0(year(date), ifelse(nchar(month(date)) == 1, paste0("0", month(date)), month(date)))) %>%
  group_by(periodo, tipo_dosis) %>%
  summarise(
    valor = sum(vacunas_aplicadas),
    .groups = "drop") %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "M") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  mutate(tipo_dosis = ifelse(is.na(tipo_dosis), "*na*", tipo_dosis))

V_GMCdg <- as.data.table(V_GMCdg) %>%
  data.table::dcast(idp + periodo + provincia + c1 + c2 + idg + departamento ~ tipo_dosis, value.var = "valor") %>%
  select(-c("*na*")) 

V_GMCdg[is.na(V_GMCdg)] <- 0
V_GMCdg$VesqC <- V_GMCdg$`2da` + V_GMCdg$Unica #variable cantidad personas con esquema completo de vacunacion


V_GMCdg <- V_GMCdg %>%
  mutate(date = ymd(paste0(substr(periodo, 1,4), "-", substr(periodo, 5,6), "-01"))) %>%
  mutate(date = date + months(1)) %>%
  mutate(date = date - days(1))

message("Generado: V_GMCdg\n")

V_GMCdg2 <- as.data.table(select(V_GMCdg, -c("c1", "periodo"))) %>%
  melt(id.vars = c("idp", "provincia", "idg", "departamento", "c2", "date"),
       measure.vars = c("1ra", "2da", "Adicional", "Refuerzo", "Unica", "VesqC")) %>%
  setnames(old = c("variable", "value"), new = c("c1", "valor"))

#Aplicaciones de vacunas agrupado por año y tipo de dosis
V_GACdg <- copy(DFC) %>%
  mutate(a_o = year(date)) %>%
  group_by(a_o, tipo_dosis) %>%
  summarise(
    valor = sum(vacunas_aplicadas),
    .groups = "drop") %>%
  mutate(c1 = "V") %>%
  mutate(c2 = "A") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  mutate(tipo_dosis = ifelse(is.na(tipo_dosis), "*na*", tipo_dosis))

V_GACdg <- as.data.table(V_GACdg) %>%
  data.table::dcast(a_o + idp + provincia + c1 + c2 + idg + departamento ~ tipo_dosis, value.var = "valor") %>%
  select(-c("*na*"))

V_GACdg[is.na(V_GACdg)] <- 0
V_GACdg$VesqC <- V_GACdg$`2da` + V_GACdg$Unica #variable cantidad personas con esquema completo de vacunacion


V_GACdg <- V_GACdg %>%
  mutate(date = ymd(paste0(a_o, "-12-31")))

message("Generado: V_GACdg\n")

V_GACdg2 <- as.data.table(select(V_GACdg, -c("c1", "a_o"))) %>%
  melt(id.vars = c("idp", "provincia", "idg", "departamento", "c2", "date"),
       measure.vars = c("1ra", "2da", "Adicional", "Refuerzo", "Unica", "VesqC")) %>%
  setnames(old = c("variable", "value"), new = c("c1", "valor"))



#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
fin0 <- Sys.time()
TIEMPO_EJECUCION <- fin0 - inicio0
message("\n# Fin de ejecución de 01_ETL_V.R en ", round(TIEMPO_EJECUCION, 2) ,"\n###################################################\n")
sink(type="message")
##############################################################################################################

close(zz0)

#limpiando
cat("--- Limpiando ---\n")
#elimino objeto innecesarios
rm(DFC)
#llamo al garbage collector
gc()
cat("Fin de ejecución de 01_ETL_V.R\n")


