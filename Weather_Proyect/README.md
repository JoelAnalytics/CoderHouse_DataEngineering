# Apache Airflow - Weather Project

- Esta carpeta contiene el DockerFile, Script Python con el ETL, Archivo YAML y el Script de Python con el DAG.
- Estos archivos componen el proyecto que se basa en la extraccion diaria de datos de una API del Clima de algunas de las ciudad mas turisticas de argentina que luego se almacena en una tabla en Redshift.
- Airflow busca orquestar cada uno de los procesos de ETL para aprovechar la interfaz grafica que tiene para ver la ejecucion 'por pasos'. Por otro lado permite de programar ejecuciones en determinado horario y asi automatizar el flujo de ETL.
