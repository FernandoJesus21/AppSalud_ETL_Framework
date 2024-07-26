

##############################################################################################################
# Script de ETL para la generacion de los dataframe principales (DFP)
#  Este script utiliza todos los DFS generados por los anteriores script
##############################################################################################################


inicio0 <- Sys.time()
options(scipen=6) #para evitar notacion cientifica
path0  <-  getwd()
script_version <- "1.0.0"
fecha_script_version <- "10-01-2024"

##############################################################################################################
zz0 <- file(paste(path0, "/salida/registro/04_ETL_EXPORT_", gsub("[:| |-]", "_", inicio0), ".txt", sep = ""), open="wt")
sink(zz0, type = "message")
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


#ARCHIVO PARA GRAFICO DE DONA
DFP_DONA_S1 <- rbind(
  C_GMP,
  C_GAP,
  CF_GAP,
  CF_GMP,
  C_GMPDe,
  C_GAPDe,
  CF_GMPDe,
  CF_GAPDe,
  V_GMP,
  V_GAP,
  V_GMPDe,
  V_GAPDe
)

message("Generado: DFP_DONA_S1\n")

#ARCHIVO PARA INDICADORES
DFP_KPI_S1 <- rbind(C_GA,
                    CF_GA,
                    C_GM,
                    CF_GM,
                    V_GM,
                    V_GA,
                    V_GAP,
                    V_GMP,
                    C_GMP, 
                    C_GAP, 
                    CF_GAP, 
                    CF_GMP,
                    V_GMPCdg2,
                    V_GAPCdg2,
                    V_GMCdg2,
                    V_GACdg2
) 

message("Generado: DFP_KPI_S1\n")

#ARCHIVO PARA GRAFICO DE SERIE TEMPORAL (DIARIO)
DFP_ST_S1 <- rbind(C_GDP, 
                   CF_GDP,
                   C_GD,
                   CF_GD,
                   V_GDP,
                   V_GD,
                   C_AC_GDP,
                   C_AC_GD,
                   CF_AC_GDP,
                   CF_AC_GD,
                   V_AC_GDP,
                   V_AC_GD
)

message("Generado: DFP_ST_S1\n")

#GRAFICO DE MAPA
DFP_MAP_S2 <- rbind(C_GMP, 
                    C_GAP,
                    C_GMPDe,
                    C_GAPDe,
                    CF_GMP,
                    CF_GAP,
                    CF_GMPDe,
                    CF_GAPDe,
                    V_GMP,
                    V_GAP,
                    V_GMPDe,
                    V_GAPDe,
                    V_GMPCdg2,
                    V_GAPCdg2,
                    V_GMCdg2,
                    V_GACdg2,
                    REL
)

message("Generado: DFP_MAP_S2\n")

#RANGO ETARIO GRAFICO DE BARRAS
DFP_RE_S2 <- rbind(C_GMPRe, 
                   C_GAPRe,
                   C_GMRe, 
                   C_GARe,
                   CF_GMPRe,
                   CF_GAPRe,
                   CF_GMRe,
                   CF_GARe,
                   V_GMPRe,
                   V_GAPRe,
                   V_GMRe,
                   V_GARe
)

DFP_RE_S2 <- DFP_RE_S2 %>%
  mutate(rango_etario = recode(rango_etario, 
                               "[0,12)" = "<12",
                               "[12,18)" = "12-17",
                               "[18,29)" = "18-29",
                               "[29,39)" = "30-39",
                               "[39,49)" = "40-49",
                               "[49,59)" = "50-59",
                               "[59,69)" = "60-69",
                               "[69,79)" = "70-79",
                               "[79,89)" = "80-89",
                               "[89,99)" = "90-99",
                               "[99,119]" = ">=100"
  )) %>%
  mutate(rango_etario = ifelse((is.na(rango_etario) | rango_etario == "S.I."), "*na*", rango_etario))

DFP_RE_S2$rango_etario <- factor(DFP_RE_S2$rango_etario,levels = c("<12","12-17","18-29","30-39","40-49","50-59","60-69","70-79","80-89","90-99",">=100", "*na*"))

message("Generado: DFP_RE_S2\n")

#grafico de mapa de calor de la seccion S3
DFP_HM_S3 <- rbind(V_GMCdg3, V_GMPCdg3)

message("Generado: DFP_HM_S3\n")

#grafico animado (provincia)
CF_GANP_S3 <- as.data.table(copy(DFP_MAP_S2)) %>%
  filter(c2 == "M") %>%
  filter(departamento == "[Todos]") %>%
  dcast(idp + provincia + idg + departamento + date + c2 ~ c1, value.var = "valor") %>%
  select(idp, idg, departamento, date, C, CF, V, RM) %>%
  left_join(provincias, by = c("idp")) %>%
  mutate(year = year(date)) %>%
  mutate(month = ifelse(nchar(month(date)) < 2, paste0("0", month(date)), as.character(month(date)))) %>%
  mutate(periodo = as.numeric(paste0(year, month))) %>%
  filter(!is.na(provincia)) %>%
  filter(!is.na(C))

CF_GANP_S3[is.na(CF_GANP_S3)] <- 0  

CF_GANP_S3 <- as.data.frame(CF_GANP_S3) %>%
  group_by(provincia) %>%
  mutate(C = cumsum(C)) %>%
  mutate(CF = cumsum(CF)) %>%
  mutate(V = cumsum(V))

message("Generado: CF_GANP_S3\n")

#grafico animado (departamento)
CF_GANDe_S3 <- as.data.table(copy(DFP_MAP_S2)) %>%
  filter(c2 == "M") %>%
  filter(departamento != "[Todos]") %>%
  dcast(idp + provincia + idg + departamento + date + c2 ~ c1, value.var = "valor") %>%
  select(idp, idg, departamento, date, C, CF, V, RM) %>%
  left_join(provincias, by = c("idp")) %>%
  mutate(year = year(date)) %>%
  mutate(month = ifelse(nchar(month(date)) < 2, paste0("0", month(date)), as.character(month(date)))) %>%
  mutate(periodo = as.numeric(paste0(year, month))) %>%
  filter(!is.na(provincia)) %>%
  filter(!is.na(C))

CF_GANDe_S3[is.na(CF_GANDe_S3)] <- 0  

CF_GANDe_S3 <- as.data.frame(CF_GANDe_S3) %>%
  group_by(idp, provincia, idg, departamento) %>%
  mutate(C = cumsum(C)) %>%
  mutate(CF = cumsum(CF)) %>%
  mutate(V = cumsum(V))

message("Generado: CF_GANDe_S3\n")

#GRAFICO ANIMADO DE LA SECCION S3
DFP_CF_GAN_S3 <- rbind(CF_GANP_S3, CF_GANDe_S3) %>%
  mutate(VCF = round((V / (CF + 1)), 2)) %>%
  mutate(RM = round(RM, 4))

message("Generado: DFP_CF_GAN_S3\n")
#################################################################################################
#SALIDA A ARCHIVO

message("Exportando archivos de salida en formato .csv\n")

write.csv2(DFP_CF_U, "salida/csv/DFP_CF_U.csv", row.names = F, fileEncoding = "UTF-8")
write.csv2(DFP_DONA_S1, "salida/csv/DFP_DONA_S1.csv", row.names = F, fileEncoding = "UTF-8")
write.csv2(DFP_MAP_S2, "salida/csv/DFP_MAP_S2.csv", row.names = F, fileEncoding = "UTF-8")
write.csv2(DFP_KPI_S1, "salida/csv/DFP_KPI_S1.csv", row.names = F, fileEncoding = "UTF-8")
write.csv2(DFP_RE_S2, "salida/csv/DFP_RE_S2.csv", row.names = F, fileEncoding = "UTF-8")
write.csv2(DFP_ST_S1, "salida/csv/DFP_ST_S1.csv", row.names = F, fileEncoding = "UTF-8")
write.csv2(DFP_CF_GAN_S3, "salida/csv/DFP_CF_GAN_S3.csv", row.names = F, fileEncoding = "UTF-8")
write.csv2(DFP_HM_S3, "salida/csv/DFP_HM_S3.csv", row.names = F, fileEncoding = "UTF-8")

message("Exportando archivos de salida en formato .fst\n")

write.fst(DFP_CF_U, "salida/fst/DFP_CF_U.fst")
write.fst(DFP_DONA_S1, "salida/fst/DFP_DONA_S1.fst")
write.fst(DFP_MAP_S2, "salida/fst/DFP_MAP_S2.fst")
write.fst(DFP_KPI_S1, "salida/fst/DFP_KPI_S1.fst")
write.fst(DFP_RE_S2, "salida/fst/DFP_RE_S2.fst")
write.fst(DFP_ST_S1, "salida/fst/DFP_ST_S1.fst")
write.fst(DFP_CF_GAN_S3, "salida/fst/DFP_CF_GAN_S3.fst")
write.fst(DFP_HM_S3, "salida/fst/DFP_HM_S3.fst")

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
fin0 <- Sys.time()
TIEMPO_EJECUCION <- fin0 - inicio0
message("\n# FIN de ejecucion de SCRIPT en ", round(TIEMPO_EJECUCION, 2) ,"\n###################################################\n")
sink(type="message")
##############################################################################################################
close(zz0)

#grabo fecha de recarga de archivos
# zt <- file("salida/reload_date.txt", open="wt")
# sink(zt, type = "message")
# message(Sys.Date())
# sink(type = "message")
# close(zt)






