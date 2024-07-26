# AppSalud ETL Framework | Proceso ETL usado por la aplicación web

Proceso de ETL que utiliza la aplicación [AppSalud](https://github.com/FernandoJesus21/AppSalud) y su [herramienta de previsión independiente](https://github.com/FernandoJesus21/AppSalud_Forecasting). Tiene por objetivo realizar ciertas transformaciones sobre los datos provistos por el Ministerio de Salud, el Instituto Geográfico Nacional (IGN) y el INDEC para que puedan ser utilizados por la aplicación principal.

![alt text](https://github.com/FernandoJesus21/AppSalud_ETL_Framework/blob/main/diagrama_ETL.png?raw=true)

# Puntos clave

1) Desarrollado en lenguaje R.
2) Contempla la corrección de errores más frecuentes (errores de entrada, errores de ambiguedad, etc.).
3) Registro de eventos a archivos de formato *.txt*.
4) Genera una salida intermedia en formato *.fst* (DFC).
5) Genera la salida final, en formato *.csv* y *.fst* (DFP).

# Fuentes de datos

1. (CSV) [Covid19Casos](https://www.datos.gob.ar/es/dataset/salud-covid-19-casos-registrados-republica-argentina) | Casos registrados en la República Argentina (hasta el 4/6/2022).
2. (CSV) [datos_nomivac_covid19](http://datos.salud.gob.ar/dataset/vacunas-contra-covid19-dosis-aplicadas-en-la-republica-argentina) | Vacunas contra COVID-19. Dosis aplicadas en la República Argentina.
3. (CSV) [Covid19_Internados_y_Fallecidos](http://datos.salud.gob.ar/dataset/covid-19-casos-registrados-en-la-republica-argentina/archivo/cfbbaf72-d79c-4a22-ac4a-13cb62c1836b) | Notificaciones de casos internados y/o fallecidos por COVID-19.
4. (CSV) [Confirmados_Ambulatorios](http://datos.salud.gob.ar/dataset/covid-19-casos-registrados-en-la-republica-argentina/archivo/fac2c863-398d-4d10-934f-31c8bc418ed9) | Notificaciones de casos de COVID-19 en pacientes ambulatorios.
5. Cantidad de habitantes por provincia obtenida del [CENSO 2022](https://censo.gob.ar/) (INDEC). El archivo se incluye en el repositorio (*/data/provincias.csv*).
6. Utiliza datos provenientes del [Instituto Geográfico Nacional](https://www.ign.gob.ar/NuestrasActividades/InformacionGeoespacial/CapasSIG). Estos también se incluyen en el repositorio (en la carpeta */data*).

# Instrucciones

1. Crear una base de datos de PostgreSQL.
2. Ejecutar el script *script_carga_poblado.sql* ubicado en */config* (modificar las rutas de los archivos .csv según corresponda).
3. Crear la carpeta */salida* en el directorio raíz con */csv*, */fst* y */registro* como subcarpetas.
4. Instalar bibliotecas y cambiar según corresponda los parámetros de conexión desde *00_ETL_START.R*.
5. Ejecutar *00_ETL_START.R*. El proceso se ejecutará e irá grabando los archivos y logs en */salida*.

*Se recomienda ejecutar en un sistema con al menos 16 GB de RAM*.

# Opciones de ejecución

Se puede modificar el valor de la variable *usar_DFC_existente* en *00_ETL_START.R* a TRUE si ya se dispone de DFC generados previamente. De lo contrario mantener su valor en FALSE.

# Tablas de la base de datos

![alt text](https://github.com/FernandoJesus21/AppSalud_ETL_Framework/blob/main/BD_tablas.png?raw=true)



