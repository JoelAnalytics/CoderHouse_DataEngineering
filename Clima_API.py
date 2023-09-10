import pandas as pd
import numpy as np
import json
import http.client
import psycopg2
from psycopg2.extras import execute_values

#Vamos a definir la Lista de Destinos turisticos que queremos monitorear

Destinos=["Buenos_Aires","Bariloche","Mendoza","Jujuy","Salta","Mar_del_plata"]

### Por medio de un Json File vamos a manejar la las Key de la API y de Redshift y tambien usuario Redshift.

with open("config.json") as config_file:
    config=json.load(config_file)

Api_Key=config["API_KEY"]
Redshift_Key=config["Reshift_KEY"]
Redshift_User=config["Redshift_User"]

#Conexion a la API de Football-API
#Vamos a crear una funcion que tome como parametros el ID del equipo, el rango de fechas que queremos bajar data y el ID de la liga

def weather_df(lista_ciudades):
    
    conn = http.client.HTTPSConnection("api.weatherapi.com")

    headers = {
        'X-RapidAPI-Key': Api_Key,
        'X-RapidAPI-Host': "api.weatherapi.com"
    }

    
    df_final=[]
    
    for x in lista_ciudades:
        code="/v1/current.json?key=34eeba99ba8e404d947211802230909&q="+str(x)+"&aqi=no"
        conn.request("GET", code , headers=headers)
        res = conn.getresponse()
        data = res.read()
        weather_df=json.loads(data)
        date=weather_df.get("location",{}).get("localtime")
        city_name=weather_df.get("location",{}).get("name")
        region=weather_df.get("location",{}).get("region")
        country=weather_df.get("location",{}).get("country")
        weather=weather_df.get("current",{}).get("temp_c")
        humidity=weather_df.get("current",{}).get("humidity")
        precipitation=weather_df.get("current",{}).get("precip_mm")
        last_updated=weather_df.get("current",{}).get("last_updated")
        registro={"Date":date,"City_Name":city_name,"Region":region,"Country":country,"Weather":weather,"Humidity":humidity,
                  "Precipitation":precipitation,"Last_Updated":last_updated}
        df_final.append(registro)
    df_final=pd.DataFrame.from_dict(df_final)
            
    return df_final

#Vamos a crear el dataframe que va devolver los datos del dia de hoy.

weather=weather_df(Destinos)

#Por medio de un jupyter hizimos la exploracion de los datos y vimos que no tiene Missing values,

df=weather.fillna(0)

#Creamos la conexion a REDSSHIFT

url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
data_base="data-engineer-database"
user=Redshift_User


try:  
    conn = psycopg2.connect(
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        dbname=data_base,
        user=user,
        password=Redshift_Key,
        port='5439')
    print("Conectado a Redshift con éxito!")

        
except Exception as e:
    print("No es posible conectar a Redshift")
    print(e)



# Crear un cursor:
#cur = conn.cursor()

# Ejecutar la sentencia DROP TABLE: 
# [Este paso lo usamos la primera vez que creamos la tabla pero no vamos a borrar nuevamente la tabla]
# cur.execute("DROP TABLE IF EXISTS Weather")

# Hacer commit para aplicar los cambios:
#conn.commit()

#Crear la tabla si no existe:
#Este paso se usa la primera vez para crear la tabla pero lo vamos a comentar. 
#No necesitamos que la cree nuevamente
# with conn.cursor() as cur:
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS Weather (
#             Date TIMESTAMP,
#             City_Name VARCHAR(20),
#             Region VARCHAR(25),
#             Country VARCHAR(25),
#             Weather FLOAT,
#             Humidity INT,
#             Precipitation FLOAT,
#             Last_Updated TIMESTAMP
#         )
#     """)
#     conn.commit()

with conn.cursor() as cur:
    execute_values(
        cur,
        '''
        INSERT INTO Weather (Date, City_Name, Region, Country, Weather, Humidity, Precipitation, Last_Updated)
        VALUES %s
        ''',
        [tuple(row) for row in weather.values],
        page_size=len(weather)
    )
    conn.commit()



# Cierro tanto el cursor como la conexión a la base de datos:
cur.close()
conn.close()