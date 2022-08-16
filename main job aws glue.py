import pandas as pd
import numpy as np
import os
from datetime import datetime
from scipy import stats
import boto3
import io
import s3fs


"""*************************************************************************************************"""
''' ************************** CREACIÓN DE PATHS NECESARIOS PARA LOS BUCKETS "************************** '''
"""*************************************************************************************************"""

#Seleccionamos el mes a normalizar, del 1 al 12
#El numero "3" está destinado para la primera carga, Marzo 2022
#Para cargas incrementales primero se debe verificar la existencia de los archivos en el bucket "raw data" en S3
month_input = "3" 

#PATHS PARA LAS DISTINTAS TABLAS QUE AYUDAN A CREAR LAS TABLAS DE ZONAS Y BOROUGH

#Paths que no cambian con el tiempo:
zones_coordinates_path_excel = "s3://raw-data-taxitrip/data/project-database/zones-coordinates/zones.csv" 
zones_lookup_path_csv = "s3://raw-data-taxitrip/data/project-database/zone-lookup/taxi+_zone_lookup.csv" 
taxi_zones_path_csv = "s3://raw-data-taxitrip/data/project-database/taxi_zones/taxi_zones.csv" 

#Paths que cambian con el tiempo:
weather_path_csv = "s3://raw-data-taxitrip/data/project-database/weather/" + month_input + "/" 
yellow_taxi_path_parquet = "s3://raw-data-taxitrip/data/project-database/yellow-taxi/" + month_input + "/" 

#Path para el bucket contenedor de los datos normalizados:
normalized_data_path = "s3://clean-data-taxitrip/data/project_database/"

"""ESTABLECEMOS USERNAME BUCKET Y ACCESS-KEY"""
AWS_S3_BUCKET = os.getenv("raw-data-taxitrip")
AWS_ACCESS_KEY_ID = os.getenv("AKIA22Q2FKRV3XOJIQOL")
AWS_SECRET_ACCESS_KEY = os.getenv("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")

AWS_ACCESS_KEY_ID = "AKIA22Q2FKRV3XOJIQOL"
AWS_SECRET_ACCESS_KEY = "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"

"""INSTANCIAMOS EL CLIENTE"""
s3_client = boto3.client(
    "s3",
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
)


"""*************************************************************************************************"""
''' ************************** CREACIÓN DE DIMENSIONES "ZONE" Y "BOROUGH "************************** '''
"""*************************************************************************************************"""

# Se cargan tablas necesarias para generar una nueva con los datos que incluiremos
zones_lat_lon = pd.read_csv(zones_coordinates_path_excel) 
zones_lookup = pd.read_csv(zones_lookup_path_csv)
taxi_zones = pd.read_csv(taxi_zones_path_csv)

# Se agregan campos que se consideran de interés a la tabla principal
zones_lookup[['Shape_Leng','Shape_Area']] = taxi_zones.merge(zones_lookup, left_on='OBJECTID', right_on='LocationID', how='right')[['Shape_Leng','Shape_Area']]
zones_lookup[['Latitud','Longitud']]= zones_lat_lon.merge(zones_lookup, on='LocationID', how='right')[['Latitud','Longitud']]

# Se crea tabla "Borough"
borough = pd.DataFrame({'BoroughID': np.arange(1,8),'Borough':np.sort(zones_lookup.Borough.unique())})

# Se reemplazan valores de columnas referenciadas por su ID
boro = borough.Borough.values
id =borough.BoroughID.values
dicc = {boro[i]:id[i] for i in range(len(boro))}
zones_lookup['Borough']= zones_lookup['Borough'].map(dicc)

# Normalización de nombres
zones_lookup.rename(columns={'Borough':'BoroughID','service_zone':'Service_Zone','Latitud':'Latitude','Longitud':'Longitude'}, inplace=True)


"""****************************************************************************************"""
''' ************************** TRANSFORMACIÓN DE TABLA WEATHER "************************** '''
"""****************************************************************************************"""

"""LECTURA DE LOS ARCHIVOS PARA LA TABLA Weather""" 
#Se crean dos listas, la primera con el nombre de los archivos dentro de la carpeta especifica del Weather 
response = s3_client.list_objects_v2(
    Bucket="raw-data-taxitrip",
    Prefix= "data/project-database/weather/" + month_input + "/") 
    
lis = [content["Key"] for content in response.get("Contents", [1])]
lis.pop(0)
#La segunda lista crea un dataframe para cada archivo y lo almacena
df_lis = [pd.read_csv("s3://raw-data-taxitrip/" + filename ) for filename in lis]
#Se concatenan todos los dataframes
Weather = pd.concat(df_lis)

""""NORMALIZACIÓN DE LA TABLA Weather"""
#Se reemplazan caracteres innecesarios del campo fecha
Weather["datetime"] = Weather["datetime"].str.replace("T"," ")
Weather["datetime"] = Weather["datetime"].str.replace("-","/")

#Convertimos el campo fecha a formato datetime
Weather["datetime"] = Weather["datetime"].apply(lambda x: datetime.strptime(x,'%Y/%m/%d %H:%M:%S'))

#Normalizamos el campo name para su posterior mapeo con ayuda de la tabla borough
Weather.name = Weather.name.str.title()
Weather["name"] = Weather["name"].str.replace("Oak Island Junction","EWR")
Weather["name"] = Weather["name"].str.replace("Queens Ny","Queens")
Weather["name"] = Weather["name"].str.replace("Manhattan ","Manhattan")

#Se rellenan los valores nulos en el campo preciptype con "No precipitation"
Weather["preciptype"] = Weather["preciptype"].fillna("No precipitation")

#Se crean las columnas que nos ayudarán a la creación de la primary key de la tabla 
Weather["Hour"] = Weather["datetime"].dt.round("H").dt.hour
Weather["Day"] = Weather["datetime"].dt.day
Weather["Year"] = Weather["datetime"].dt.year
Weather["Month"] = Weather["datetime"].dt.month

#Se crea un diccionario para el mapeo de los ID's de los boroughs con ayuda de la tabla borough
map_dict = {borough.Borough.values[i]:borough.BoroughID.values[i] for i in range(len(borough.BoroughID.values))}
Weather["BoroughID"] = Weather["name"].map(map_dict)

#Creación de la primary key de la tabla, para ella se consideran campos de tiempo y el borough
Weather["WeatherID"] = Weather["Hour"].map(str) + "X" + Weather["Day"].map(str) +"X" + Weather["Month"].map(str) + "X" + Weather["Year"].map(str) + "X" + Weather["BoroughID"].map(str)

#Dropeamos del dataset las columnas que no nos interesan
Weather.drop(columns=["dew","sealevelpressure","solarenergy","stations","Hour","Day","Year","Month","name"], inplace=True)

#Asigno un dato a un valor float por si no hay nieve, para que al momento de hacer update en athena no me de error por el tipo de dato de la columna
Weather["snow"] = Weather["snow"].astype(np.float64)
Weather["snow"].values[0] = 0.2  
Weather["snowdepth"] = Weather["snowdepth"].astype(np.float64)
Weather["snowdepth"].values[0] = 0.2


#Renombramos las columnas
Weather.rename(columns={"datetime":"Datetime",
                     'temp':"Temp", 
                     'feelslike':"Feels_Like",
                     'humidity':"Humidity", 
                     'precip':"Precip", 
                     'precipprob':"Precip_Prob",
                     'preciptype':"Precip_Type", 
                     'snow':"Snow", 
                     'snowdepth':"Snow_Depth", 
                     'windgust': "Windgust", 
                     'windspeed': "Wind_Speed", 
                     'winddir': "Wind_direction",
                     'cloudcover': "Cloud_Cover", 
                     "visibility": "Visibility", 
                     'solarradiation': "Solar_Radiation", 
                     'uvindex': "UV_Index", 
                     'severerisk': "Severe_Risk",
                     'conditions': "Conditions", 
                     'icon': "Icon"},
            inplace=True)


#Cambiamos de posicion las columnas
column_to_move = Weather.pop("WeatherID")
column_to_move1 = Weather.pop("Datetime")
column_to_move2 = Weather.pop("BoroughID")
Weather.insert(0, "WeatherID", column_to_move)
Weather.insert(1, "Datetime", column_to_move1)
Weather.insert(2, "BoroughID", column_to_move2)

"""****************************************************************************************"""
"""****************************************************************************************"""
''' ************************** TRANSFORMACIÓN DE TABLA TAXI TRIP *************************** '''
"""****************************************************************************************"""
"""****************************************************************************************"""

df = pd.read_parquet(yellow_taxi_path_parquet + "yellow_tripdata_2022-0" + month_input + ".parquet", engine="pyarrow")
# Se desactivan viajes con valores nulos.
df.loc[df.isnull().any(1), 'Activado'] = 0

""""COLUMNA VENDORID"""
# Valores que no pertenecen a los previstos en el diccionario de datos se categorizan como '9'.
df.loc[~df.VendorID.isin([1,2]),'VendorID'] = 9
# Se desactivan VendorID que sean distintos a 1 y 2.
df.loc[((df['VendorID']!=1) & (df['VendorID']!=2)), 'Activado'] = 0


""""COLUMNA PICK UP Y DROP OFF DATETIME"""
# Se desactivan fechas de PU que no sean de 2022.
df.loc[df['tpep_pickup_datetime'].dt.year != 2022, 'Activado'] = 0
# Se desactivan fechas de PU que no sean del mes a normalizar
df.loc[df['tpep_pickup_datetime'].dt.month != int(month_input), 'Activado'] = 0
# Se desactivan fechas donde DO ocurre antes de PU.
df.loc[(df['tpep_pickup_datetime']>df['tpep_dropoff_datetime']), 'Activado'] = 0

# Creación del tiempo de duración del viaje, en formado Fecha y hora, y en segundos.
df['Duration_Time'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime'])
df['Duration_Secs'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.seconds
# Se desactivan viajes con duración menor a 30 segundos.
df.loc[df['Duration_Secs']<=30, 'Activado'] = 0
# Se desactivan viajes con duración mayor o igual a un día.
df.loc[df['Duration_Time'].dt.days>=1, 'Activado'] = 0

""""COLUMNA PASSENGER COUNT"""
# Se desactivan los viajes con 0 pasajeros o más de 7 pasajeros.
df.loc[((df['passenger_count']==0) | (df['passenger_count']>7)), 'Activado'] = 0

""""COLUMNA TRIP DISTANCE"""
#Se cambian las unidades de millas a km
df["trip_distance"] = df["trip_distance"]*1.60934
# Se desactivan viajes con distancias iguales a 0.
df.loc[df['trip_distance']==0, 'Activado'] = 0

""""COLUMNA RATECOREID"""
# Valores que no pertenecen a los previstos en el diccionario de datos se categorizan como '9'.
# y se desactivan.
df.loc[df['RatecodeID']==99, 'Activado'] = 0

""""COLUMNA STORE AND FOWARD FLAG"""
# Se recodifican los valores 'Y' y 'N' en 1 y 0, en otro caso se clasifica como '9'.
# Este cambio se almacena en otra variable.
df['StoreForward_ID'] = np.where(df['store_and_fwd_flag']=='Y',1,np.where(df['store_and_fwd_flag']=='N',0,9))
df.drop('store_and_fwd_flag', axis=1, inplace=True)
# Se desactivan los viajes '9'.
df.loc[df['StoreForward_ID']==9, 'Activado'] = 0

""""COLUMNA PAYMENT TYPE"""
# Valores que no pertenecen a los previstos en el diccionario de datos se categorizan como '9'.
df.loc[~df.payment_type.isin([1,2,3,4,5,6]), 'payment_type'] = 9 
# Se desactivan los viajes '9'.
df.loc[df['payment_type']==9, 'Activado'] = 0

""""COLUMNA FARE AMOUNT"""
# Se desactivan tarifas menores a 2.5.
# 2.5 es la tarifa base, ningun valor debe ser menor a esta (https://www1.nyc.gov/site/tlc/passengers/taxi-fare.page)
df.loc[df['fare_amount']<2.5, 'Activado'] = 0

""""COLUMNA EXTRA"""
# Extra: Varios extras y recargos. Actualmente, esto solo incluye los cargos de $0.50 y $1 por la hora pico y por la noche.
# Se desactivan valores negativos.
df.loc[df['extra']<0, 'Activado'] = 0

""""COLUMNA MTA TAX"""
# mta_tax: Impuesto MTA de $0.50 que se activa automáticamente en función de la tarifa medida en uso.
# Se desactivan valores distintos a 0.5.
df.loc[df['mta_tax']!=0.5, 'Activado'] = 0

""""COLUMNA TIP AMOUNT"""
# tip_amount: Importe de la propina: este campo se completa automáticamente para las propinas de tarjetas de crédito. Las propinas en efectivo no están incluidas.
# Se deactivan valores negativos.
df.loc[df['tip_amount']<0, 'Activado'] = 0

""""COLUMNA TOLLS AMOUNT"""
# tolls_amount: Importe total de todos los peajes pagados en el viaje.
# Se deactivan valores negativos.
df.loc[df['tolls_amount']<0, 'Activado'] = 0

""""COLUMNA IMPROVEMENT SURCHARGE"""
# Se desactiva todo valor diferente a 0.3, ya que este es el único valor posible para este campo según https://www.nytimes.com/2014/05/01/nyregion/city-approves-30-surcharge-to-pay-for-accessible-taxis_db.html
df.loc[df['improvement_surcharge']!=0.3, 'Activado'] = 0

""""COLUMNA TOTAL AMOUNT"""
# total amount: Monto total del viaje.
# Se deactivan valores negativos.
df.loc[df['total_amount']<0, 'Activado'] = 0

"""COLUMNA CONGESTION SURCHARGE"""
# Se desactiva todo valor diferente a 0 o 2.5, ya que estos valores son los establecidos para taxis_db amarillos en https://www1.nyc.gov/site/tlc/about/congestion-surcharge.page
df.loc[~df.congestion_surcharge.isin([0,2.5]), 'Activado'] = 0

"""COLUMNA AIRPORT FEE"""
# Se desactiva todo valor diferente a 0 o 1.25, ya que estos valores son los establecidos.
df.loc[~df.airport_fee.isin([0,1.25]), 'Activado'] = 0

#Todo viaje que no esté categorizado como desactivado (valor 0) se asigna como activado (valor 1)
df.loc[df['Activado']!=0, 'Activado'] = 1

"""CREACION DE COLUMNA TOTAL AMOUNT FIXED"""
#Se crea esta nueva columna, considerando los datos activados en este cálculo.
df["Total_Amount_Fixed"] = (df.fare_amount + df.extra + df.mta_tax + df.tolls_amount + df.improvement_surcharge + df.congestion_surcharge + df.airport_fee)*df['Activado']


"""CATEGORIZACIÓN DE OUTLIERS"""
# Se considera el métodos de las 3 sigmas, para las 3 variables escalas importantes.
df["Duration_Outlier"] = np.where((stats.zscore(df["Duration_Secs"]))>=3,1,0)
df["Distance_Outlier"] = np.where((stats.zscore(df["trip_distance"]))>=3,1,0)
df["Total_Amount_Fixed_Outlier"] = np.where((stats.zscore(df["Total_Amount_Fixed"]))>=3,1,0)
df["Total_Amount_Outlier"] = np.where((stats.zscore(df["total_amount"]))>=3,1,0)

"""NORMALIZACIÓN DE NOMBRES"""
df.rename(columns={ 'tpep_pickup_datetime': 'PU_Datetime',
                    'tpep_dropoff_datetime': 'DO_Datetime',
                    'passenger_count': 'Passenger_Count',
                    'trip_distance': 'Trip_Distance',
                    'payment_type': 'Payment_TypeID',
                    'fare_amount': 'Fare_Amount',
                    'extra': 'Extra',
                    'mta_tax': 'Mta_Tax',
                    'tip_amount': 'Tip_Amount',
                    'tolls_amount': 'Tolls_Amount',
                    'improvement_surcharge': 'Improvement_Surcharge',
                    'total_amount': 'Total_Amount',
                    'congestion_surcharge': 'Congestion_Surcharge',
                    'airport_fee': 'Airport_Fee'}, 
        inplace=True)

df['Activado']=df.Activado.astype(int)
df['Distance_Outlier']=df.Distance_Outlier.astype(int)
df['Duration_Outlier']=df.Duration_Outlier.astype(int)

"""CREACION DE LAS COLUMNAS NECESARIAS PARA LA FOREIGN KEY DEL CLIMA"""
#Se crean las columnas que nos ayudarán a la creación de la primary key de la tabla 
df["Hour"] = df["PU_Datetime"].dt.round("H").dt.hour
df["Day"] = df["PU_Datetime"].dt.day
df["Year"] = df["PU_Datetime"].dt.year
df["Month"] = df["PU_Datetime"].dt.month

#Se crea un diccionario para el mapeo de los ID's de los boroughs con ayuda de la tabla zones_lookup
map_taxi = {zones_lookup.LocationID.values[i]:zones_lookup.BoroughID.values[i] for i in range(len(zones_lookup.BoroughID.values))}
df["BoroughID"] = df["PULocationID"].map(map_taxi)

#Creación de la primary key de la tabla, para ella se consideran campos de tiempo y el borough
df["WeatherID"] = df["Hour"].map(str) + "X" + df["Day"].map(str) +"X" + df["Month"].map(str) + "X" + df["Year"].map(str) + "X" + df["BoroughID"].map(str)

#Dropeamos del dataset las columnas que no necesitamos
df.drop(columns=["Hour","Day","Year","Month","BoroughID", "Total_Amount_Outlier"], inplace=True)

"""****************************************************************************************"""
''' ************************** CREACION DE LAS TABLAS DIMENSIONALES "************************** '''
"""****************************************************************************************"""

#Se crean las tablas si es la primera ingesta
if int(month_input) == 3:

    vendor = pd.DataFrame({"VendorID":[1,2,9],"Vendor":["Creative Mobile Technologies","VeriFone Inc.","Sin Dato"]})
    rate_code = pd.DataFrame({"RatecodeID":[1,2,3,4,5,6,9],"Ratecode":["Standard Rate", "JFK", "Newark", "Nassau or Westchester", "Negotiated Fare", "Group Ride", "Sin Dato"]})
    payment_type = pd.DataFrame({"Payment_TypeID":[1,2,3,4,5,6,9], "Payment": ["Credit Card", "Cash", "No Charge", "Dispute", "Unknown", "Voided Trip", "Sin Dato"]})
    storeforward = pd.DataFrame({"StoreForward_ID": [0,1,9],"Storeforward":["Store and forward trip", "Not a store and forward trip","Sin Dato"]})
else:
    pass


"""****************************************************************************************"""
''' ************************** ALMACENAMIENTO EN S3 "************************** '''
"""****************************************************************************************"""

normalized_data_path = "s3://clean-data-taxitrip/data/project_database/"

#Siempre se envian al clean bucket las tablas que cambian con el tiempo

df.to_csv(normalized_data_path + "yellow-taxi/" + month_input + "/" + "yellow-tripdata-" + month_input + "-" + "2022.csv", index=False, encoding="utf-8", header=True)
Weather.to_csv(normalized_data_path + "weather/" + month_input + "/" + "weather-" + month_input + "-" + "2022.csv", index=False, encoding="utf-8", header=True)

#Se envian a S3 las tablas que no cambian con el tiempo solo si es la primera ingesta
if int(month_input) == 3:

    vendor.to_csv("s3://clean-data-taxitrip/data/project_database/vendor/vendor.csv", index=False, encoding="utf-8", header=True)
    borough.to_csv("s3://clean-data-taxitrip/data/project_database/borough/borough.csv", index=False, encoding="utf-8", header=True)
    rate_code.to_csv("s3://clean-data-taxitrip/data/project_database/rate_code/rate_code.csv", index=False, encoding="utf-8", header=True)
    payment_type.to_csv("s3://clean-data-taxitrip/data/project_database/payment_type/payment_type.csv", index=False, encoding="utf-8", header=True)
    storeforward.to_csv("s3://clean-data-taxitrip/data/project_database/storeforward/storeforward.csv", index=False, encoding="utf-8", header=True)
    zones_lookup.to_csv("s3://clean-data-taxitrip/data/project_database/taxi_zones/zones.csv", index=False, encoding="utf-8", header=True)

else:
    print("Las tablas taxi_trip y weather del mes " + month_input + " acaban de ser normalizadas y subidas a S3 \n")


print("All done baby")
