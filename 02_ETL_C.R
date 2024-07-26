
##############################################################################################################
# Script de ETL para la tabla h_covid_casos
# Este script se encarga de procesar el conjunto de los casos confirmados
##############################################################################################################

inicio0 <- Sys.time()
options(scipen=6) #para evitar notacion cientifica
path0  <-  getwd()
script_version <- "3.0.0"
fecha_script_version <- "13-01-2024"

##############################################################################################################
zz0 <- file(paste(path0, "/salida/registro/02_ETL_C_", gsub("[:| |-]", "_", inicio0), ".txt", sep = ""), open="wt")
sink(zz0, type = "message")
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


#Comienzo de mensajes
message("02_ETL_C: \n")
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
  message("Importando DFC_C.fst\n")
  DFC <- read.fst("salida/fst/DFC_C.fst", as.data.table = T)
  
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

#Casos confirmados agrupado por mes, provincia y departamento
C_GMPDe <- copy(DFC) %>%
  group_by(mes, idp, provincia, idg, departamento) %>%
  summarise(date = max(date),
            valor = sum(confirmados),
            .groups = "drop") %>%
  mutate(c1 = "C") %>%
  mutate(c2 = "M") %>%
  select(-mes)

message("Generado: C_GMPDe\n")

#Casos confirmados agrupado por año, provincia y departamento
C_GAPDe <- copy(DFC) %>%
  group_by(year(date), idp, provincia, idg, departamento) %>%
  summarise(date = max(date),
            valor = sum(confirmados),
            .groups = "drop") %>%
  mutate(c1 = "C") %>%
  mutate(c2 = "A") %>%
  setnames(old = c("year(date)"), new = c("a_o")) %>%
  select(-a_o)

message("Generado: C_GAPDe\n")

#Casos confirmados agrupado por día y provincia
C_GDP <- copy(DFC) %>%
  group_by(date, idp, provincia) %>%
  summarise(valor = sum(confirmados),
            .groups = "drop") %>%
  mutate(semana =  1 + as.numeric(date - as.Date("2020-03-01")) %/% 7) %>%
  mutate(mes = paste0(year(date), ifelse(nchar(month(date)) == 1, paste0("0", month(date)), month(date)))) %>%
  mutate(c1 = "C") %>%
  mutate(c2 = "D") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-c("semana", "mes"))

message("Generado: C_GDP\n")

#Casos confirmados acumulados agrupado por dia y provincia
C_AC_GDP <- copy(C_GDP) %>%
  group_by(idp, provincia, c1, c2, idg, departamento) %>%
  arrange(date) %>%
  mutate(valor = cumsum(valor)) %>%
  mutate(c1 = "C_AC")

message("Generado: C_AC_GDP\n")

#Casos confirmados agrupado por dia
C_GD <- copy(DFC) %>%
  group_by(date) %>%
  summarise(valor = sum(confirmados),
            .groups = "drop") %>%
  mutate(c1 = "C") %>%
  mutate(c2 = "D") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]")

message("Generado: C_GD\n")

#Casos confirmados acumulados agrupado por dia
C_AC_GD <- copy(C_GD) %>%
  group_by(idp, provincia, c1, c2, idg, departamento) %>%
  arrange(date) %>%
  mutate(valor = cumsum(valor)) %>%
  mutate(c1 = "C_AC")

message("Generado: C_AC_GD\n")

#Casos confirmados agrupado por semana y provincia
C_GSP <- copy(DFC) %>%
  group_by(semana, idp, provincia) %>%
  summarise(date = min(date),
            valor = sum(confirmados),
            .groups = "drop") %>%
  mutate(c1 = "C") %>%
  mutate(c2 = "S") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-semana)

message("Generado: C_GSP\n")

#Casos confirmados agrupado por semana
C_GS <- copy(DFC) %>%
  group_by(semana) %>%
  summarise(date = min(date),
            valor = sum(confirmados),
            .groups = "drop") %>%
  mutate(c1 = "C") %>%
  mutate(c2 = "S") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-semana)

message("Generado: C_GS\n")

#Casos confirmados agrupado por mes y provincia
C_GMP <- copy(DFC) %>%
  group_by(mes, idp, provincia) %>%
  summarise(date = max(date),
            valor = sum(confirmados),
            .groups = "drop") %>%
  mutate(c1 = "C") %>%
  mutate(c2 = "M")%>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-mes)

message("Generado: C_GMP\n")

#Casos confirmados agrupado por mes
C_GM <- copy(DFC) %>%
  group_by(mes) %>%
  summarise(date = max(date),
            valor = sum(confirmados),
            .groups = "drop")  %>%
  mutate(c1 = "C") %>%
  mutate(c2 = "M") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-mes)

message("Generado: C_GM\n")

#Casos confirmados agrupado por año
C_GA <- copy(DFC) %>%
  group_by(year(date)) %>%
  summarise(date = max(date),
            valor = sum(confirmados),
            .groups = "drop") %>%
  setnames(old = c("year(date)"), new = c("a_o")) %>%
  mutate(c1 = "C") %>%
  mutate(c2 = "A") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-a_o)

message("Generado: C_GA\n")

#Casos confirmados agrupado por año y provincia
C_GAP <- copy(DFC) %>%
  group_by(year(date), idp, provincia) %>%
  summarise(date = max(date),
            valor = sum(confirmados),
            .groups = "drop") %>%
  setnames(old = c("year(date)"), new = c("a_o")) %>%
  mutate(c1 = "C") %>%
  mutate(c2 = "A") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-a_o)

message("Generado: C_GAP\n")

#dataframe auxiliar para generar otros DFS
aux_Re <- copy(DFC) %>%
  filter(edad != "*na*") %>%
  mutate(edad = as.numeric(edad)) %>%
  filter(edad != -1)

aux_Re$rango_etario <- cut(aux_Re$edad, 
                         breaks = c(0, 12, 18, 29, 39, 49, 59, 69, 79, 89, 99, 119),
                         include.lowest = T,
                         right = F)


#Casos confirmados agrupado por mes, provincia y rango etario
C_GMPRe <- copy(aux_Re) %>%
  mutate(rango_etario = as.character(rango_etario)) %>%
  mutate(periodo = paste0(year(date), month(date))) %>%
  group_by(periodo, idp, provincia, rango_etario) %>%
  summarise(
    date = max(date),
    valor = sum(confirmados),
    .groups = "drop"
  ) %>%
  mutate(c1 = "C") %>%
  mutate(c2 = "M") %>%
  mutate(rango_etario = ifelse(is.na(rango_etario), "*na*", rango_etario))
C_GMPRe <- C_GMPRe[,-1]

message("Generado: C_GMPRe\n")

#Casos confirmados agrupado por año, provincia y rango etario
C_GAPRe <- copy(aux_Re) %>%
  mutate(rango_etario = as.character(rango_etario)) %>%
  group_by(year(date), idp, provincia, rango_etario) %>%
  summarise(
    date = max(date),
    valor = sum(confirmados),
    .groups = "drop"
  ) %>%
  mutate(c1 = "C") %>%
  mutate(c2 = "A") %>%
  setnames(old = "year(date)", new = "a_o") %>%
  mutate(rango_etario = ifelse(is.na(rango_etario), "*na*", rango_etario))
C_GAPRe <- C_GAPRe[,-1]

message("Generado: C_GAPRe\n")

#Casos confirmados agrupado por mes y rango etario
C_GMRe <- copy(aux_Re) %>%
  mutate(rango_etario = as.character(rango_etario)) %>%
  mutate(periodo = paste0(year(date), month(date))) %>%
  group_by(periodo, rango_etario) %>%
  summarise(
    date = max(date),
    valor = sum(confirmados),
    .groups = "drop"
  ) %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(c1 = "C") %>%
  mutate(c2 = "M") %>%
  mutate(rango_etario = ifelse(is.na(rango_etario), "*na*", rango_etario))
C_GMRe <- C_GMRe[,-1]

message("Generado: C_GMRe\n")

#Casos confirmados agrupado por año y rango etario
C_GARe <- copy(aux_Re) %>%
  mutate(rango_etario = as.character(rango_etario)) %>%
  group_by(year(date), rango_etario) %>%
  summarise(
    date = max(date),
    valor = sum(confirmados),
    .groups = "drop"
  ) %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(c1 = "C") %>%
  mutate(c2 = "A") %>%
  setnames(old = "year(date)", new = "a_o") %>%
  mutate(rango_etario = ifelse(is.na(rango_etario), "*na*", rango_etario))
C_GARe <- C_GARe[,-1]

message("Generado: C_GARe\n")


#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
fin0 <- Sys.time()
TIEMPO_EJECUCION <- fin0 - inicio0
message("\n# Fin de ejecución de 01_ETL_C.R en ", round(TIEMPO_EJECUCION, 2) ,"\n###################################################\n")
sink(type="message")
##############################################################################################################

close(zz0)

#limpiando
cat("--- Limpiando ---\n")
#elimino objeto innecesarios
rm(aux_Re, DFC)
#llamo al garbage collector
gc()
cat("Fin de ejecución de 01_ETL_C.R\n")

