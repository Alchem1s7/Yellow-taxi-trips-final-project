
# path = input('Ingrese ruta de acceso los datasets: ')
# user_name = input('Nombre de usuario MySQL (Ejemplo: root): ')
# user_pass = input('Contraseña del usuario MySQL : ')
# database_name = input('Nombre de la base de datos MySQL: ')

'''CREACIÓN DE TABLAS EN MYSQL'''
from sqlalchemy import create_engine
import pymysql
from simpledbf import Dbf5
import pandas as pd
import numpy as np
import os
from datetime import datetime
from scipy import stats
import mysql.connector
from mysql.connector import Error
import sqlalchemy
from sympy import root
from pathlib import Path
import chardet


connection=pymysql.connect(
    host='localhost',
    port=3306,
    user= 'root',
    password= 'root',
    db= 'taxis',
    local_infile=True,
)
cursor=connection.cursor()
cursor.execute('set sql_safe_updates=0')
cursor.execute('SET GLOBAL read_only=0')
cursor.execute('use taxis') 

#Se dropean las tablas referenciadas para evitar problemas con foreign key
cursor.execute("DROP TABLE IF EXISTS taxi_trip")
cursor.execute("DROP TABLE IF EXISTS zone")

cursor.execute('DROP TABLE IF EXISTS vendor')
cursor.execute('''CREATE TABLE vendor (
                                    VendorID INT, 
                                    Vendor VARCHAR(30), 
                                    primary Key (VendorID)
                                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4  COLLATE=utf8mb4_spanish_ci'''
                )
cursor.execute('INSERT INTO vendor (VendorID, Vendor) VALUES (1,"Creative Mobile Technologies"), (2,"VeriFone Inc."),(9,"Sin Dato")')
print('Creacion de dimension "vendor".......... OK')

cursor.execute('DROP TABLE IF EXISTS rate_code')
cursor.execute('''CREATE TABLE rate_code (RatecodeID INT, 
                                        Ratecode VARCHAR(30),
                                        primary Key (RatecodeID)
                                        )ENGINE=InnoDB DEFAULT CHARSET=utf8mb4  COLLATE=utf8mb4_spanish_ci'''
                )
cursor.execute('INSERT INTO rate_code (RatecodeID, Ratecode) VALUES (1,"Standard Rate"), (2,"JFK"), (3,"Newark"), (4,"Nassau or Westchester"), (5,"Negotiated Fare"), (6,"Group Ride"),(99,"Sin Dato")') 
print('Creacion de dimension "rate_code".......... OK')

cursor.execute('DROP TABLE IF EXISTS payment_type')
cursor.execute('''CREATE TABLE payment_type (Payment_TypeID INT, 
                                            Payment_Type VARCHAR(30),
                                            primary Key (Payment_TypeID)
                                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4  COLLATE=utf8mb4_spanish_ci'''
                )
cursor.execute('INSERT INTO payment_type (Payment_TypeID,Payment_Type) VALUES (1,"Credit Card"), (2,"Cash"), (3,"No Charge"), (4,"Dispute"), (5,"Unknown"), (6,"Voided Trip"),(9,"Sin Dato")')
print('Creacion de dimension "payment_type".......... OK')

cursor.execute('DROP TABLE IF EXISTS storeforward')
cursor.execute('''CREATE TABLE storeforward (StoreForward_ID INT, 
                                            StoreForward VARCHAR(30), 
                                            primary Key (StoreForward_ID)
                                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4  COLLATE=utf8mb4_spanish_ci'''
                )
cursor.execute('INSERT INTO storeforward (StoreForward_ID,StoreForward) VALUES (0,"Store and forward trip"),(1,"Not a store and forward trip"),(9,"Sin Dato")')
print('Creacion de dimension "storeforward".......... OK')

cursor.execute('DROP TABLE IF EXISTS borough')
cursor.execute('''CREATE TABLE borough (BoroughID INT , 
                                        Borough VARCHAR(50),
                                        primary key (BoroughID)
                                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4  COLLATE=utf8mb4_spanish_ci'''
                )
print('Creacion de dimension "borough".......... OK')

cursor.execute('DROP TABLE IF EXISTS zone')
cursor.execute('''CREATE TABLE zone (LocationID	INT, 
                                    BoroughID INT, 
                                    Zone VARCHAR(60), 
                                    Service_Zone VARCHAR(60), 
                                    Shape_Leng DOUBLE, 
                                    Shape_Area	DOUBLE, 
                                    Latitude DOUBLE, 
                                    Longitude DOUBLE,
                                    primary key (LocationID),
                                    foreign key (BoroughID) references borough (BoroughID)
                                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4  COLLATE=utf8mb4_spanish_ci'''
                )
print('Creacion de dimension "zone".......... OK')

cursor.execute('DROP TABLE IF EXISTS weather')
cursor.execute('''CREATE TABLE weather(WeatherID VARCHAR(20), 
                                        Datetime DATE, 
                                        BoroughID INT, 
                                        Temp DECIMAL(5,2), 
                                        Feels_Like DECIMAL(5,2), 
                                        Humidity DECIMAL(5,2), 
                                        Precip DECIMAL(5,2), 
                                        Precip_Prob DECIMAL(5,2), 
                                        Precip_Type VARCHAR(50), 
                                        Snow DECIMAL(4,2), 
                                        Snow_Depth DECIMAL(4,2), 
                                        Windgust DECIMAL(5,2), 
                                        Wind_Speed DECIMAL(5,2), 
                                        Wind_direction DECIMAL(5,2), 
                                        Cloud_Cover DECIMAL(6,3), 
                                        Visibility DECIMAL(5,2), 
                                        Solar_Radiation INT, 
                                        UV_Index INT, 
                                        Severe_Risk INT, 
                                        Conditions VARCHAR(30), 
                                        Icon VARCHAR(40),
                                        primary key (WeatherID),
                                        foreign key (BoroughID) references borough (BoroughID)
                                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4  COLLATE=utf8mb4_spanish_ci'''
                ) 
print('Creacion de dimension "weather".......... OK')

cursor.execute('DROP TABLE IF EXISTS taxi_trip')
cursor.execute('''CREATE TABLE taxi_trip(TripID INT AUTO_INCREMENT,	
                                        VendorID INT, 
                                        PU_Datetime DATETIME,	
                                        DO_Datetime	DATETIME, 
                                        Passenger_Count INT, 
                                        Trip_Distance DECIMAL(15,2),	
                                        RatecodeID	INT, 
                                        PULocationID INT, 
                                        DOLocationID INT, 
                                        Payment_TypeID INT, 
                                        Fare_Amount DECIMAL(15,2), 
                                        Extra DECIMAL(15,2), 
                                        Mta_Tax DECIMAL(15,2), 
                                        Tip_Amount DECIMAL(15,2), 
                                        Tolls_Amount DECIMAL(15,2), 
                                        Improvement_Surcharge DECIMAL(15,2), 
                                        Total_Amount DECIMAL(15,2), 
                                        Congestion_Surcharge DECIMAL(15,2), 
                                        Airport_Fee DECIMAL(15,2), 
                                        Activado INT, 
                                        Duration_Time VARCHAR(40), 
                                        Duration_Secs INT, 
                                        StoreForward_ID INT, 
                                        Total_Amount_Fixed DECIMAL(15,2), 
                                        Duration_Outlier INT, 
                                        Distance_Outlier INT, 
                                        Total_Amount_Fixed_Outlier DECIMAL(15,2), 
                                        WeatherID VARCHAR(20),
                                        primary Key (TripID),
                                        foreign key (VendorID) references vendor (VendorID),
                                        foreign key (RatecodeID) references rate_code (RatecodeID),
                                        foreign key (PULocationID) references zone (LocationID),
                                        foreign key (DOLocationID) references zone (LocationID),
                                        foreign key (Payment_TypeID) references payment_type (Payment_TypeID),
                                        foreign key (StoreForward_ID) references storeforward (StoreForward_ID)
                                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4  COLLATE=utf8mb4_spanish_ci'''
                )
print('Creacion de dimension "taxi_trip".......... OK')


connection.commit()
connection.close()
print('--------------------------------------------------------')

'''CREACIÓN DE DIMENSIONES "ZONE" Y "BOROUGH" '''

# Se cargan tablas necesarias para generar una nueva con los datos que incluiremos
zones_lat_lon = pd.read_excel('./datasets/zones.xlsx') 
zones_lookup = pd.read_csv('./datasets/taxi+_zone_lookup.csv')

dbf = Dbf5('./datasets/taxi_zones.dbf')
taxi_zones = dbf.to_dataframe()

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

print("Las tablas zone y borough se han creado correctamente")
print("-"*100)

'''INGESTA EN TABLAS: ZONE Y BOROUGH'''
pymysql.install_as_MySQLdb()

engine = create_engine('mysql+mysqldb://root:root@localhost/taxis', echo = False)


borough.to_sql(name='borough', con=engine, if_exists='append', index=False)
print('Ingesta en tabla "borough".......... OK')

zones_lookup.to_sql(name='zone', con=engine, if_exists='append', index=False)
print('Ingesta en tabla "zone".......... OK')


'''TRANSFORMACIÓN DE TABLA WEATHER''' 
"""INGESTA DE LOS ARCHIVOS PARA LA TABLA Weather"""
'''
#Se crean dos listas, la primera con el nombre de los archivos dentro de la carpeta especifica del Weather
lis = [file for file in os.listdir('./Datasets clima') if file.endswith(".csv")]
#La segunda lista crea un dataframe para cada archivo y lo almacena
df_lis = [pd.read_csv('\\Datasets clima\\' + filename ) for filename in lis]
#Se concatenan todos los dataframes
Weather = pd.concat(df_lis)
'''

ds_path = Path('Datasets clima') # Path to the datasets folder

# Concatenar todos los datasets de venta dentro de uno
Weather_list = []
for file in ds_path.glob('*.csv'):
    codificacion = chardet.detect(open(file, 'rb').read())['encoding']
    df = pd.read_csv(file, delimiter=',', decimal='.', encoding=codificacion)
    Weather_list.append(df)
Weather = pd.concat(Weather_list)
print("La tabla weather se concateno exitosamente")

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

'''INGESTA EN TABLA WEATHER'''

pymysql.install_as_MySQLdb()

engine = create_engine('mysql+mysqldb://root:root@localhost/taxis', echo = False)


Weather.to_sql(name='weather', con=engine, if_exists='append', index=False)
print('Ingesta en tabla "weather".......... OK')

'''RELACIONES ENTRE TABLAS'''

'''TRANSFORMACIÓN DE TABLA TAXI_TRIP'''


# Carga de datos
df = pd.read_parquet("./datasets/yellow_tripdata_2022-03.parquet", "pyarrow")

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
# Se desactivan fechas de PU que no sean de marzo.
df.loc[df['tpep_pickup_datetime'].dt.month != 3, 'Activado'] = 0
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
# Se desactiva todo valor diferente a 0.3, ya que este es el único valor posible para este campo según https://www.nytimes.com/2014/05/01/nyregion/city-approves-30-surcharge-to-pay-for-accessible-taxis.html
df.loc[df['improvement_surcharge']!=0.3, 'Activado'] = 0

""""COLUMNA TOTAL AMOUNT"""
# total amount: Monto total del viaje.
# Se deactivan valores negativos.
df.loc[df['total_amount']<0, 'Activado'] = 0

"""COLUMNA CONGESTION SURCHARGE"""
# Se desactiva todo valor diferente a 0 o 2.5, ya que estos valores son los establecidos para taxis amarillos en https://www1.nyc.gov/site/tlc/about/congestion-surcharge.page
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
print("Normalizacion de la tabla taxi_trip exitosa")

'''INGESTA EN TABLA TAXI_TRIP POR LOTES'''

# Se crea un iterable para hacer un ingesta por tramos a través de un bucle for
batch=list(np.arange(0,df.shape[0],100000))
batch.append(df.shape[0])
'''
# Ingesta en lotes de 100.000 registros
for i,j in enumerate(batch):
  if i == 0:
    None
  else:
    data = df[batch[i-1]:batch[i]]

    try:
        pymysql.install_as_MySQLdb()
    
        engine = create_engine('mysql+mysqldb://root:root@localhost/taxis', echo = False)
        
        data.to_sql(name='taxi_trip', con=engine, if_exists='append', index=False)
        print(f'Ingesta de registros {j-1} a {j}.......... OK')

    except:
        print(f'Ingesta de registros {j-1} a {j}.......... FAIL')
    finally:
        print('----------------------------------------------------------')


host_name,db_name, u_name, u_pass, port_num  = "localhost","nyc", "root", "fdlr1719", "3306"
'''

'''
#Conexion a la db
mydb = mysql.connector.connect(
  host='localhost',
  user='root',
  password='root',
  database='taxis'
)

#Creacion del motor de base de datos
engine = create_engine("mysql+mysqlconnector://" + u_name + ":" + u_pass + "@" 
                        + host_name + ":" + port_num + "/" + db_name, echo=False)


#Importacion del dataframe a nuestra base de datos en SQL
df.to_sql(name="taxi_trip", con=engine, if_exists="append", index=False)

print("All done")
'''
pymysql.install_as_MySQLdb()

engine = create_engine('mysql+mysqldb://root:root@localhost/taxis', echo = False)

# Cambiar nombres de los DataFrame y las tablas.
df[:500000].to_sql(name='taxi_trip', con=engine, if_exists='append', index=False)
df[500000:1000000].to_sql(name='taxi_trip', con=engine, if_exists='append', index=False)
df[1000000:1500000].to_sql(name='taxi_trip', con=engine, if_exists='append', index=False)
df[1500000:2000000].to_sql(name='taxi_trip', con=engine, if_exists='append', index=False)
df[2000000:2500000].to_sql(name='taxi_trip', con=engine, if_exists='append', index=False)
df[2500000:3000000].to_sql(name='taxi_trip', con=engine, if_exists='append', index=False)
df[3000000:].to_sql(name='taxi_trip', con=engine, if_exists='append', index=False)


