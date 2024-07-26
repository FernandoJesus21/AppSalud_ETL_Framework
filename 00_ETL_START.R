
##############################################################################################################
# Script de inicio del módulo ETL
# Este script ejecuta ordenadamente todos los pasos del ETL

#ENTRADAS: 
# h_covid_casos (BBDD)
# h_covid_vacunados (BBDD)
# internaciones_y_fallecidos (BBDD)
# conf_ambulatorios (BBDD)
# provincias.csv
# indice_pcias_deptos_completo.csv

#SALIDAS:

# Todos los DFP
# DFC de los casos confirmados (C), confirmados fallecidos (CF) y vacunaciones (V)

#carga de paquetes
library(readr)
library(dplyr)
library(lubridate)
library(data.table)
library(sqldf)
library(RPostgreSQL)
library(RPostgres)
library(DBI)
library(fasttime)
library(fst)
library(tidyr)


#Parametros de conexión a la BD
DB_NAME <- 'postgres'
HOST_DB <- '127.0.0.1'
DB_PORT <- '5432'
DB_USER <- 'postgres'  
DB_PASS <- 'admin'

##############################################################################################################

#permite elegir usar DFC que se haya generado con anterioridad
# Si es T: va a buscar los archivos DFC_C, DFC_CF y DFC_V (no requerirá conexion con BBDD)
# Si es F: se conecta a la BBDD trayendo todos los registros correspondientes, 
#   realiza las transformaciones generando los DFC, a continuación los exportara como archivos fst y sigue con los demás procesos hasta finalizar.
usar_DFC_existente <- F


#ETAPA 1: ejecución de los script auxiliares (constantes y funciones de limpieza y corrección de datos)
source("01_ETL_CONST.R")
Sys.sleep(.1)
source("01_ETL_DATA_cL.R")
Sys.sleep(.1)

#ETAPA 2: ejecución de los script principales de extracción y transformación de datos para generar los DFS de C, CF y V

#extraccion de datos si el flag usar_DFC_existente es T
if(!usar_DFC_existente){
  source("01_ETL_DB_C.R")
  Sys.sleep(.1)
  source("01_ETL_DB_CF.R")
  Sys.sleep(.1)
  source("01_ETL_DB_V.R")
  Sys.sleep(.1)
}

#lectura a partir de DFC existentes si el flag es T
source("02_ETL_C.R")
Sys.sleep(.1)
source("02_ETL_CF.R")
Sys.sleep(.1)
source("02_ETL_V.R")
Sys.sleep(.1)


#ETAPA 3: ejecución del script para la generacion de los DFS con variables relativas
source("03_ETL_REL.R")
Sys.sleep(.1)

#ETAPA 4: ejecución del script que ensambla los DFP y los exporta
source("04_ETL_EXPORT.R")
Sys.sleep(.1)

