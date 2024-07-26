
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
zz0 <- file(paste(path0, "/salida/registro/02_ETL_CF_", gsub("[:| |-]", "_", inicio0), ".txt", sep = ""), open="wt")
sink(zz0, type = "message")
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


#Comienzo de mensajes
message("02_ETL_CF: \n")
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
  message("Importando DFC_CF.fst\n")
  DFC <- read.fst("salida/fst/DFC_CF.fst", as.data.table = T)
  
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



#Casos confirmados que fallecieron agrupado por mes, provincia y departamento
CF_GMPDe <- copy(DFC) %>%
  group_by(mes, idp, provincia, idg, departamento) %>%
  summarise(date = max(date),
            valor = sum(fallecidos),
            .groups = "drop") %>%
  mutate(c1 = "CF") %>%
  mutate(c2 = "M") %>%
  select(-mes)

message("Generado: CF_GMPDe\n")

#Casos confirmados que fallecieron agrupado por año, provincia y departamento
CF_GAPDe <- copy(DFC) %>%
  group_by(year(date), idp, provincia, idg, departamento) %>%
  summarise(date = max(date),
            valor = sum(fallecidos),
            .groups = "drop") %>%
  mutate(c1 = "CF") %>%
  mutate(c2 = "A") %>%
  setnames(old = c("year(date)"), new = c("a_o")) %>%
  select(-a_o)

message("Generado: CF_GAPDe\n")

#Casos confirmados que fallecieron agrupado por dia y provincia
CF_GDP <- copy(DFC) %>%
  group_by(date, idp, provincia) %>%
  summarise(valor = sum(fallecidos),
            .groups = "drop") %>%
  filter(date > as.Date("2020-03-01")) %>% #quito caso mal registrado
  mutate(c1 = "CF") %>%
  mutate(c2 = "D") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]")

message("Generado: CF_GDP\n")

#Casos confirmados que fallecieron acumulados agrupado por dia y provincia 
CF_AC_GDP <- copy(CF_GDP) %>%
  group_by(idp, provincia, c1, c2, idg, departamento) %>%
  arrange(date) %>%
  mutate(valor = cumsum(valor)) %>%
  mutate(c1 = "CF_AC")

message("Generado: CF_AC_GDP\n")

#Casos confirmados que fallecieron agrupado por dia
CF_GD <- copy(DFC) %>%
  group_by(date) %>%
  summarise(valor = sum(fallecidos),
            .groups = "drop") %>%
  mutate(c1 = "CF") %>%
  mutate(c2 = "D") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]")

message("Generado: CF_GD\n")

#Casos confirmados que fallecieron acumulados agrupado por dia
CF_AC_GD <- copy(CF_GD) %>%
  group_by(idp, provincia, c1, c2, idg, departamento) %>%
  arrange(date) %>%
  mutate(valor = cumsum(valor)) %>%
  mutate(c1 = "CF_AC")

message("Generado: CF_AC_GD\n")

#Casos confirmados que fallecieron agrupado por semana y provincia
CF_GSP <- copy(DFC) %>%
  group_by(semana, idp, provincia) %>%
  summarise(date = min(date),
            valor = sum(fallecidos),
            .groups = "drop") %>%
  mutate(c1 = "CF") %>%
  mutate(c2 = "S") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-semana)

message("Generado: CF_GSP\n")

#Casos confirmados que fallecieron agrupado por semana
CF_GS <- copy(DFC) %>%
  group_by(semana) %>%
  summarise(date = min(date),
            valor = sum(fallecidos),
            .groups = "drop") %>%
  mutate(c1 = "CF") %>%
  mutate(c2 = "S") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-semana)

message("Generado: CF_GS\n")

#Casos confirmados que fallecieron agrupado por mes y provincia
CF_GMP <- copy(DFC) %>%
  group_by(mes, idp, provincia) %>%
  summarise(date = max(date),
            valor = sum(fallecidos),
            .groups = "drop") %>%
  mutate(c1 = "CF") %>%
  mutate(c2 = "M") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-mes)

message("Generado: CF_GMP\n")

#Casos confirmados que fallecieron agrupado por mes
CF_GM <- copy(DFC) %>%
  group_by(mes) %>%
  summarise(date = max(date),
            valor = sum(fallecidos),
            .groups = "drop") %>%
  mutate(c1 = "CF") %>%
  mutate(c2 = "M") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-mes)

message("Generado: CF_GM\n")


#Casos confirmados que fallecieron agrupado por año
CF_GA <- copy(DFC) %>%
  group_by(year(date)) %>%
  summarise(date = max(date),
            valor = sum(fallecidos),
            .groups = "drop") %>%
  setnames(old = c("year(date)"), new = c("a_o")) %>%
  mutate(c1 = "CF") %>%
  mutate(c2 = "A") %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-a_o)

message("Generado: CF_GA\n")


#Casos confirmados que fallecieron agrupado por año y provincia
CF_GAP <- copy(DFC) %>%
  group_by(year(date), idp, provincia) %>%
  summarise(date = max(date),
            valor = sum(fallecidos),
            .groups = "drop") %>%
  setnames(old = c("year(date)"), new = c("a_o")) %>%
  mutate(c1 = "CF") %>%
  mutate(c2 = "A") %>%
  mutate(idg = "0") %>%
  mutate(departamento = "[Todos]") %>%
  select(-a_o)

message("Generado: CF_GAP\n")

#decesos por edad y provincia DESAGRUPADO
DFP_CF_U <- copy(DFC) %>%
  select(date, edad, idp, provincia, fallecidos) 

DFP_CF_U <- data.frame(DFP_CF_U[rep(seq_len(dim(DFP_CF_U)[1]), with(DFP_CF_U, ifelse(fallecidos > 0 & !is.na(fallecidos), fallecidos, 1))), , drop = FALSE], row.names=NULL) %>%
  select(-fallecidos) %>%
  filter(edad != -1)

DFP_CF_U <- DFP_CF_U %>%
  filter(edad != "*na*") %>%
  mutate(edad = as.numeric(edad)) %>%
  filter(edad < 122) 

message("Generado: DFP_CF_U\n")


#dataframe auxiliar para generar otros DFS

aux_Re <- copy(DFP_CF_U) %>%
  filter(edad != "*na*") %>%
  mutate(edad = as.numeric(edad))

aux_Re$rango_etario <- cut(aux_Re$edad, 
                          breaks = c(0, 12, 18, 29, 39, 49, 59, 69, 79, 89, 99, 119),
                          include.lowest = T,
                          right = F)


#Casos confirmados que fallecieron agrupado por mes, provincia y rango etario
CF_GMPRe <- copy(aux_Re) %>%
  mutate(rango_etario = as.character(rango_etario)) %>%
  mutate(periodo = paste0(year(date), month(date))) %>%
  group_by(periodo, idp, provincia, rango_etario) %>%
  summarise(
    date = max(date),
    valor = n(),
    .groups = "drop"
  ) %>%
  mutate(c1 = "CF") %>%
  mutate(c2 = "M") 
CF_GMPRe <- CF_GMPRe[,-1]

message("Generado: CF_GMPRe\n")

#Casos confirmados que fallecieron agrupado por año, provincia y rango etario
CF_GAPRe <- copy(aux_Re) %>%
  mutate(rango_etario = as.character(rango_etario)) %>%
  group_by(year(date), idp, provincia, rango_etario) %>%
  summarise(
    date = max(date),
    valor = n(),
    .groups = "drop"
  ) %>%
  mutate(c1 = "CF") %>%
  mutate(c2 = "A")
CF_GAPRe <- CF_GAPRe[,-1]

message("Generado: CF_GAPRe\n")


#Casos confirmados que fallecieron  agrupado por mes y rango etario
CF_GMRe <- copy(aux_Re) %>%
  mutate(rango_etario = as.character(rango_etario)) %>%
  mutate(periodo = paste0(year(date), month(date))) %>%
  group_by(periodo, rango_etario) %>%
  summarise(
    date = max(date),
    valor = n(),
    .groups = "drop"
  ) %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(c1 = "CF") %>%
  mutate(c2 = "M") 
CF_GMRe <- CF_GMRe[,-1]

message("Generado: CF_GMRe\n")

#Casos confirmados que fallecieron agrupado por año y rango etario
CF_GARe <- copy(aux_Re) %>%
  mutate(rango_etario = as.character(rango_etario)) %>%
  group_by(year(date), rango_etario) %>%
  summarise(
    date = max(date),
    valor = n(),
    .groups = "drop"
  ) %>%
  mutate(idp = "0") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(c1 = "CF") %>%
  mutate(c2 = "A")
CF_GARe <- CF_GARe[,-1]

message("Generado: CF_GARe\n")



#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
fin0 <- Sys.time()
TIEMPO_EJECUCION <- fin0 - inicio0
message("\n# Fin de ejecución de 02_ETL_CF.R en ", round(TIEMPO_EJECUCION, 2) ,"\n###################################################\n")
sink(type="message")
##############################################################################################################

close(zz0)

#limpiando
cat("--- Limpiando ---\n")
#elimino objeto innecesarios
rm(aux_Re, DFC)
#llamo al garbage collector
gc()
cat("Fin de ejecución de 02_ETL_CF.R\n")

