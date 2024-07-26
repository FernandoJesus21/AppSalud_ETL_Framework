
##############################################################################################################
# Script de ETL para la generacion de los dataframe con variables combinadas/relativas
#  Este script utiliza todos los DFS generados por los anteriores script
##############################################################################################################


inicio0 <- Sys.time()
options(scipen=6) #para evitar notacion cientifica
path0  <-  getwd()
script_version <- "1.0.0"
fecha_script_version <- "10-01-2024"

##############################################################################################################
zz0 <- file(paste(path0, "/salida/registro/03_ETL_REL_", gsub("[:| |-]", "_", inicio0), ".txt", sep = ""), open="wt")
sink(zz0, type = "message")
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


#CONFIRMADOS FALLECIDOS agrupado por PROVINCIA y MES
CF_REL_GMP <- copy(CF_GMP) %>%
  setnames(old = "valor", new = "fallecidos") %>%
  mutate(periodo = paste0(year(date), ifelse(nchar(month(date)) == 1, paste0("0", month(date)), month(date)))) %>%
  select(-c("date", "c1", "c2"))

message("Generado: CF_REL_GMP\n")

#CONFIRMADOS FALLECIDOS agrupado por PROVINCIA, DEPTO y MES
CF_REL_GMPDe <- copy(CF_GMPDe) %>%
  setnames(old = "valor", new = "fallecidos") %>%
  mutate(periodo = paste0(year(date), ifelse(nchar(month(date)) == 1, paste0("0", month(date)), month(date)))) %>%
  select(-c("date", "c1", "c2"))

message("Generado: CF_REL_GMPDe\n")

#CONFIRMADOS agrupado por PROVINCIA y MES
C_REL_GMP <- copy(C_GMP) %>%
  setnames(old = "valor", new = "confirmados") %>%
  mutate(periodo = paste0(year(date), ifelse(nchar(month(date)) == 1, paste0("0", month(date)), month(date)))) %>%
  select(-c("c1", "c2", "date"))

message("Generado: C_REL_GMP\n")

#CONFIRMADOS agrupado por PROVINCIA, DEPTO y MES
C_REL_GMPDe <- copy(C_GMPDe) %>%
  setnames(old = "valor", new = "confirmados") %>%
  mutate(periodo = paste0(year(date), ifelse(nchar(month(date)) == 1, paste0("0", month(date)), month(date)))) %>%
  select(-c("c1", "c2", "date"))

message("Generado: C_REL_GMPDe\n")

#VACUNADOS agrupado por PROVINCIA Y MES
V_REL_GMP <- copy(V_GMP) %>%
  setnames(old = "valor", new = "vacunados") %>%
  mutate(periodo = paste0(year(date), ifelse(nchar(month(date)) == 1, paste0("0", month(date)), month(date)))) %>%
  select(-c("c1", "c2", "date")) 

message("Generado: V_REL_GMP\n")

#VACUNADOS agrupado por PROVINCIA, DEPTO Y MES
V_REL_GMPDe <- copy(V_GMPDe) %>%
  setnames(old = "valor", new = "vacunados") %>%
  mutate(periodo = paste0(year(date), ifelse(nchar(month(date)) == 1, paste0("0", month(date)), month(date)))) %>%
  select(-c("c1", "c2", "date")) 

message("Generado: V_REL_GMPDe\n")


V_REL_GMP <- V_REL_GMP %>%
  left_join(select(V_GMPCdg, -c("c1", "c2", "idg", "departamento", "date")), by=c("idp", "provincia", "periodo"))

message("Generado: V_REL_GMP\n")

V_REL_GMPDe <- V_REL_GMPDe %>%
  left_join(select(V_GMPDeCdg, -c("c1", "c2", "date")), by=c("idp", "provincia", "idg", "departamento", "periodo"))

message("Generado: V_REL_GMPDe\n")

################################################################
#ARMADO DEL DF DE VARIABLES RELATIVAS PRINCIPAL (POR PROVINCIA)
################################################################

REL_GMP <- left_join(C_REL_GMP, CF_REL_GMP, by=c("idp", "provincia", "idg", "departamento", "periodo")) %>%
  left_join(V_REL_GMP, by=c("idp", "provincia", "idg", "departamento", "periodo")) %>%
  left_join(provincias, by = c("idp", "provincia")) %>%
  corregirPcias_SI() %>%
  filter(provincia != "*na*")
REL_GMP[is.na(REL_GMP)] <- 0

message("Generado: REL_GMP\n")

#VARIABLES RELATIVAS
REL_GMP$RM <- ifelse(REL_GMP$confirmados != 0, REL_GMP$fallecidos / REL_GMP$confirmados, 0)
REL_GMP$RC100 <- ifelse(!is.na(REL_GMP$poblacion), ((100000 * REL_GMP$confirmados) / REL_GMP$poblacion), NA)
REL_GMP$RCF100 <- ifelse(!is.na(REL_GMP$fallecidos), ((100000 * REL_GMP$fallecidos) / REL_GMP$poblacion), NA)
REL_GMP$R1ra <- ifelse(REL_GMP$vacunados != 0, REL_GMP$`1ra` / REL_GMP$vacunados, 0)
REL_GMP$R2da <- ifelse(REL_GMP$vacunados != 0, REL_GMP$`2da` / REL_GMP$vacunados, 0)
REL_GMP$Radic <- ifelse(REL_GMP$vacunados != 0, REL_GMP$Adicional / REL_GMP$vacunados, 0)
REL_GMP$Rref <- ifelse(REL_GMP$vacunados != 0, REL_GMP$Refuerzo / REL_GMP$vacunados, 0)
REL_GMP$Runic <- ifelse(REL_GMP$vacunados != 0, REL_GMP$Unica / REL_GMP$vacunados, 0)

message("-- Creadas variables relativas para REL_GMP\n")

#DF Formato vertical
REL_GMP <- as.data.table(REL_GMP) %>%
  select(-c("vacunados", "confirmados", "fallecidos", "poblacion")) %>%
  melt(id.vars = c("idp", "provincia", "idg", "departamento", "periodo"),
       measure.vars = c("RM", "RC100", "RCF100", "R1ra", "R2da", "Radic", "Rref", "Runic"))


REL_GMP <- setnames(REL_GMP, old = c("variable", "value"), new = c("c1", "valor")) %>%
  mutate(c2 = "M") %>%
  mutate(date = ymd(paste0(substr(periodo, 1,4), "-", substr(periodo, 5,6), "-01"))) %>%
  mutate(date = date + months(1)) %>%
  mutate(date = date - days(1))

message("Generado: REL_GMP\n")

#CREACION DE DF preeliminar GAP
REL_GAP <- copy(REL_GMP) %>%
  group_by(year(date), idp, provincia, idg, departamento, c1, c2) %>%
  summarise(
    date = max(date),
    valor = mean(valor),
    .groups = "drop"
  ) %>%
  mutate(c2 = "A")
REL_GAP <- REL_GAP[,-1]

message("Generado: REL_GAP\n")

#CREACION DE DF preeliminar GM
REL_GM <- copy(REL_GMP) %>%
  #mutate(periodo = paste0(year(date), month(date))) %>%
  group_by(periodo, c1, c2) %>%
  summarise(
    date = max(date),
    valor = mean(valor),
    .groups = "drop"
  ) %>%
  mutate(provincia = "[Todas]") %>%
  mutate(idp = "0") %>%
  mutate(departamento = "[Todos]") %>%
  mutate(idg = "0")
REL_GM <- REL_GM[,-1]

message("Generado: REL_GM\n")

#CREACION DE DF preeliminar GA
REL_GA <- copy(REL_GM) %>%
  group_by(year(date), c1, c2) %>%
  summarise(
    date = max(date),
    valor = mean(valor),
    .groups = "drop"
  ) %>%
  mutate(c2 = "A") %>%
  mutate(provincia = "[Todas]") %>%
  mutate(idp = "0") %>%
  mutate(departamento = "[Todos]") %>%
  mutate(idg = "0")
REL_GA <- REL_GA[,-1]

message("Generado: REL_GA\n")

REL_GMP <- select(REL_GMP, -periodo)


REL_P <- rbind(REL_GA, REL_GAP, REL_GM, REL_GMP)

####################################################################
#ARMADO DEL DF DE VARIABLES RELATIVAS PRINCIPAL (POR DEPARTAMENTO) #falta corregir a partir de aca
####################################################################

REL_GMPDe <- left_join(C_REL_GMPDe, CF_REL_GMPDe, by=c("idp", "provincia", "idg", "departamento", "periodo")) %>%
  left_join(V_REL_GMPDe, by=c("idp", "provincia", "idg", "departamento", "periodo")) %>%
  left_join(provincias, by = c("idp", "provincia")) %>%
  corregirPcias_SI() %>%
  filter(provincia != "*na*")
REL_GMPDe[is.na(REL_GMPDe)] <- 0

message("Generado: REL_GMPDe\n")

#VARIABLES RELATIVAS
REL_GMPDe$RM <- ifelse(REL_GMPDe$confirmados != 0, REL_GMPDe$fallecidos / REL_GMPDe$confirmados, 0)
REL_GMPDe$RC100 <- ifelse(!is.na(REL_GMPDe$poblacion), ((100000 * REL_GMPDe$confirmados) / REL_GMPDe$poblacion), NA)
REL_GMPDe$RCF100 <- ifelse(!is.na(REL_GMPDe$fallecidos), ((100000 * REL_GMPDe$fallecidos) / REL_GMPDe$poblacion), NA)
REL_GMPDe$R1ra <- ifelse(REL_GMPDe$vacunados != 0, REL_GMPDe$`1ra` / REL_GMPDe$vacunados, 0)
REL_GMPDe$R2da <- ifelse(REL_GMPDe$vacunados != 0, REL_GMPDe$`2da` / REL_GMPDe$vacunados, 0)
REL_GMPDe$Radic <- ifelse(REL_GMPDe$vacunados != 0, REL_GMPDe$Adicional / REL_GMPDe$vacunados, 0)
REL_GMPDe$Rref <- ifelse(REL_GMPDe$vacunados != 0, REL_GMPDe$Refuerzo / REL_GMPDe$vacunados, 0)
REL_GMPDe$Runic <- ifelse(REL_GMPDe$vacunados != 0, REL_GMPDe$Unica / REL_GMPDe$vacunados, 0)

message("-- Creadas variables relativas para REL_GMPDe\n")

#DF Formato vertical
REL_GMPDe <- as.data.table(REL_GMPDe) %>%
  select(-c("vacunados", "confirmados", "fallecidos", "poblacion")) %>%
  melt(id.vars = c("idp", "provincia", "idg", "departamento", "periodo"),
       measure.vars = c("RM", "RC100", "RCF100", "R1ra", "R2da", "Radic", "Rref", "Runic"))

REL_GMPDe <- setnames(REL_GMPDe, old = c("variable", "value"), new = c("c1", "valor")) %>%
  mutate(c2 = "M") %>%
  mutate(date = ymd(paste0(substr(periodo, 1,4), "-", substr(periodo, 5,6), "-01"))) %>%
  mutate(date = date + months(1)) %>%
  mutate(date = date - days(1))

message("Generado: REL_GMPDe\n")

#CREACION DE DF preeliminar GAPDe
REL_GAPDe <- copy(REL_GMPDe) %>%
  group_by(year(date), idp, provincia, idg, departamento, c1, c2) %>%
  summarise(
    date = max(date),
    valor = mean(valor),
    .groups = "drop"
  ) %>%
  mutate(c2 = "A")
REL_GAPDe <- REL_GAPDe[,-1]

message("Generado: REL_GAPDe\n")

REL_GMPDe <- select(REL_GMPDe, -periodo)


REL_De <- rbind(REL_GAPDe, REL_GMPDe)


######################################################################################################

#JUNTA PARA ARMAR EL DF REL DEFINITIVO:

REL <- rbind(REL_De, REL_P)

message("Generado: REL\n")

##################################################################


#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
fin0 <- Sys.time()
TIEMPO_EJECUCION <- fin0 - inicio0
message("\n# FIN de ejecucion de SCRIPT en ", round(TIEMPO_EJECUCION, 2) ,"\n###################################################\n")
sink(type="message")
##############################################################################################################
close(zz0)

