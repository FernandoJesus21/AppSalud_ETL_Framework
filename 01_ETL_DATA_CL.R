
##############################################################################################################
# Script de definicion de funciones auxiliares para la limpieza de datos
# Este script es necesario ejecutarse antes de la etapa 2 de ejecucion
##############################################################################################################

#Correccion de la mal carga de los departamentos respecto a las provincias
corregirCasosAmbiguos <- function(df){
  
  df <- df %>%
    mutate(departamento = ifelse(departamento == "COMUNA 1", "COMUNA 01", departamento)) %>%
    mutate(departamento = ifelse(departamento == "COMUNA 2", "COMUNA 02", departamento)) %>%
    mutate(departamento = ifelse(departamento == "COMUNA 3", "COMUNA 03", departamento)) %>%
    mutate(departamento = ifelse(departamento == "COMUNA 4", "COMUNA 04", departamento)) %>%
    mutate(departamento = ifelse(departamento == "COMUNA 5", "COMUNA 05", departamento)) %>%
    mutate(departamento = ifelse(departamento == "COMUNA 6", "COMUNA 06", departamento)) %>%
    mutate(departamento = ifelse(departamento == "COMUNA 7", "COMUNA 07", departamento)) %>%
    mutate(departamento = ifelse(departamento == "COMUNA 8", "COMUNA 08", departamento)) %>%
    mutate(departamento = ifelse(departamento == "COMUNA 9", "COMUNA 09", departamento)) %>%
    mutate(provincia = ifelse(departamento == "Coronel de Marina L. Rosales", "Buenos Aires", provincia)) %>%
    mutate(provincia = ifelse(departamento == "Hurlingham", "Buenos Aires", provincia)) %>%
    mutate(provincia = ifelse(departamento == "Campana", "Buenos Aires", provincia)) %>%
    mutate(provincia = ifelse(departamento == "COMUNA 01", "CABA", provincia)) %>%
    mutate(provincia = ifelse(departamento == "COMUNA 02", "CABA", provincia)) %>%
    mutate(provincia = ifelse(departamento == "COMUNA 03", "CABA", provincia)) %>%
    mutate(provincia = ifelse(departamento == "COMUNA 04", "CABA", provincia)) %>%
    mutate(provincia = ifelse(departamento == "COMUNA 05", "CABA", provincia)) %>%
    mutate(provincia = ifelse(departamento == "COMUNA 06", "CABA", provincia)) %>%
    mutate(provincia = ifelse(departamento == "COMUNA 07", "CABA", provincia)) %>%
    mutate(provincia = ifelse(departamento == "COMUNA 08", "CABA", provincia)) %>%
    mutate(provincia = ifelse(departamento == "COMUNA 09", "CABA", provincia)) %>%
    mutate(provincia = ifelse(departamento == "COMUNA 10", "CABA", provincia)) %>%
    mutate(provincia = ifelse(departamento == "COMUNA 11", "CABA", provincia)) %>%
    mutate(provincia = ifelse(departamento == "COMUNA 12", "CABA", provincia)) %>%
    mutate(provincia = ifelse(departamento == "COMUNA 13", "CABA", provincia)) %>%
    mutate(provincia = ifelse(departamento == "COMUNA 14", "CABA", provincia)) %>%
    mutate(provincia = ifelse(departamento == "COMUNA 15", "CABA", provincia)) %>%
    mutate(provincia = ifelse(departamento == "Lanús", "Buenos Aires", provincia)) %>%
    mutate(provincia = ifelse(departamento == "12 de Octubre", "Chaco", provincia)) %>%
    mutate(provincia = ifelse(departamento == "Ezeiza", "Buenos Aires", provincia)) %>%
    mutate(provincia = ifelse(departamento == "Baradero", "Buenos Aires", provincia))
  
  return(df)
  
}

#se encontro que en los datos de las provincias originales hay varias etiquetas para definir un caso no identificado. Se reemplazan estas etiqutetas por una sola
corregirPcias_SI <- function(df){
  df <- df %>%
    mutate(provincia = ifelse(provincia == "SIN ESPECIFICAR" | provincia == "S.I." | is.na(provincia), "*na*", provincia))
}

#se encontro que en los datos de los deptos originales hay varias etiquetas para definir un caso no identificado. Se reemplazan estas etiqutetas por una sola
corregirDeptos_SI <- function(df){
  df <- df %>%
    mutate(departamento = ifelse(departamento == "SIN ESPECIFICAR" | departamento == "S.I." | is.na(departamento), "*na*", departamento))
}

#se encontro que en los datos de los rangos etarios originales hay varias etiquetas para definir un caso no identificado. Se reemplazan estas etiqutetas por una sola
corregirRangoEtario_SI <- function(df){
  df <- df %>%
    mutate(rango_etario = ifelse(rango_etario == "SIN ESPECIFICAR" | rango_etario == "S.I." | is.na(rango_etario), "*na*", rango_etario))
}


