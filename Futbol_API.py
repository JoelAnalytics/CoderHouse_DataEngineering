import pandas as pd
import numpy as np
import json
import http.client
import psycopg2
from psycopg2.extras import execute_values

### Por medio de un Json File vamos a manejar la las Key de la API y de Redshift y tambien usuario Redshift.

with open("config.json") as config_file:
    config=json.load(config_file)

Api_Key=config["API_KEY"]
Redshift_Key=config["Reshift_KEY"]
Redshift_User=config["Redshift_User"]

#Conexion a la API de Football-API
#Vamos a crear una funcion que tome como parametros el ID del equipo, el rango de fechas que queremos bajar data y el ID de la liga

def create_df_stats(team_id,fecha_inicio,fecha_fin,league):
    
    conn = http.client.HTTPSConnection("v3.football.api-sports.io")

    headers = {
        'x-rapidapi-host': "v3.football.api-sports.io",
        'x-rapidapi-key': Api_Key
        }
    
    df_final=[]
    
    for x in range(fecha_inicio,fecha_fin+1):
        code="/teams/statistics?season="+str(x)+"&team="+str(team_id)+"&league="+str(league)
        conn.request("GET", code , headers=headers)
        res = conn.getresponse()
        stats_data = res.read()
        teams_df=json.loads(stats_data)
        #Definiendo variables relevantes
        team_name=teams_df["response"]["team"]["name"]
        ##Definiendo Df de Goles a Favor y Goles en Contra en Memoria que no requiere llamada extra de la API
        goals_for=teams_df["response"]["goals"]["for"]["minute"]
        goals_against=teams_df["response"]["goals"]["against"]["minute"]
        yellow_card=teams_df["response"]["cards"]["yellow"]
        red_card=teams_df["response"]["cards"]["red"]
        
        #El orden de las Keys del diccionario a Favor y en Contra no deberia Cambiar para juntar la data por Minutos.
        #Vamos a chequear si no hubo algun cambio para no poner goles en rangos equivocados de minutos antes de avanzar
        if list(goals_for.keys())==list(goals_against.keys()):
            
            for minute_for in goals_for.keys():
                range_minutes=minute_for
                total_goals=goals_for[minute_for]["total"]
                total_goals_against=goals_against[minute_for]["total"]
                total_yellow_card=yellow_card[minute_for]["total"]
                total_red_card=red_card[minute_for]["total"]
                registro={"Year":x,"Team":team_name,"Minutes":range_minutes,"Goals_For":total_goals,
                          "Goals_Against":total_goals_against,"Yellow_card":total_yellow_card,"Red_card":total_red_card}
                df_final.append(registro)            
        else:
            print("Los minutos no Matchean")
            
    df_final=pd.DataFrame.from_dict(df_final)
            
    return df_final

#Vamos a crear el dataframe que va recolectar los datos historicos hasta el dia de hoy.
df=create_df_stats(435,2015,2016,128)

#Por medio de un jupyter hizimos la exploracion de los datos y vimos que tiene Missing values,
#Por lo tanto dentro del script se va agregar un paso de limpieza, donde hay nulls es porque no hubo gol
#O porque no hubo tarjeta por lo que es equivalente a 0.

df=df.fillna(0)

#Se va transformar el tipo de dato para que quede bien

df["Goals_For"]=df["Goals_For"].astype(int)
df["Goals_Against"]=df["Goals_Against"].astype(int)
df["Yellow_card"]=df["Yellow_card"].astype(int)
df["Red_card"]=df["Red_card"].astype(int)
df["Goals_Against"]=df["Goals_Against"].astype(int)
df["Goals_Against"]=df["Goals_Against"].astype(int)
df["Year"] = pd.to_datetime(df["Year"], format="%Y")
df["Year"]=df["Year"].dt.year


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
cur = conn.cursor()

# Ejecutar la sentencia DROP TABLE:
cur.execute("DROP TABLE IF EXISTS Goals")

# Hacer commit para aplicar los cambios:
conn.commit()

#Crear la tabla si no existe:

with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Goals (
            Year INT,
            Team VARCHAR(255),
            Minutes VARCHAR(10),
            Goals_For INT,
            Goals_Against INT,
            Yellow_Card INT,
            Red_card INT
        )
    """)
    conn.commit()

with conn.cursor() as cur:
    execute_values(
        cur,
        '''
        INSERT INTO Goals (Year, Team, Minutes, Goals_For, Goals_Against,Yellow_Card,Red_card)
        VALUES %s
        ''',
        [tuple(row) for row in df.values],
        page_size=len(df)
    )
    conn.commit()



# Cierro tanto el cursor como la conexión a la base de datos:
cur.close()
conn.close()