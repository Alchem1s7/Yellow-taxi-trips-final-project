import pandas as pd
import plotly.express as px
import streamlit as st
#
#CREACION DE LOS DATAFRAMES
df_trips_por_borough = pd.read_csv("./main/trips_por_borough.csv")
df_distancias_mas_largas = pd.read_csv("./main/dist_mas_largas.csv")
df_distancias_mas_cortas = pd.read_csv("./main/dist_mas_cortas.csv")
df_pasajeros_por_pu = pd.read_csv("./main/avg_pasajeros.csv")
df_viajes_mas_frecuentes = pd.read_csv("./main/viajes_frecuentes.csv")

#CREACION DE LAS TABLAS CON PLOTLY Y DESPLEGUE A STREAMLIT

st.title("""Vista rápida de los KPIs & Prediccion de demanda""")
st.header("""Proyecto final yellow taxi trips""")
st.write("""Esta app tiene por objetivo mostrar una vista rapida de los KPIs, asi como tambien darle la posibilidad al usuario de interactuar y predecir la demanda de taxis según el Borough""")

#Concentracion de viajes por borough

st.title("""Concentracion de viajes dentro de cada borough""")

fig2 = px.bar(df_trips_por_borough, x = "borough", y = "numero_de_viajes", title = "Numero de viajes por borough", labels={"borough":"Borough","numero_de_viajes":"Cantidad de viajes"},color='numero_de_viajes',
          color_continuous_scale=px.colors.sequential.RdBu)
fig2 = px.pie(df_trips_por_borough, values='numero_de_viajes', names='borough', title='Viajes por borough')
st.plotly_chart(fig2)
with st.expander("See explanation"):
     st.write("""
     Manhattan es el distrito que mas densidad de población posee, y es de esperar que sea el distrito con mayor movilidad respecto a viajes de taxis amarillos. En 2019 la comision de taxis de NY aprobó un cargo por congestion a todo aquel viaje que inicie, termine o pase por debajo de la avenida 96 ubicada en Manhattan. Este distrito debe de ser considerado como base para el despliegue de los servicios de transporte.
     """)

#Viajes con distancias mas largas segun punto de partida

st.title("""Zonas donde la distancia promedio de los viajes es mas grande""")
fig3 = px.bar(df_distancias_mas_largas, x = "zona", y = "distancia_promedio", title = "Zonas con distancias mas grandes en promedio", labels={"zona":"Zona de partida","distancia_promedio":"Promedio de distancias (Km)"},color='distancia_promedio',
          color_continuous_scale=px.colors.sequential.Blackbody)
st.plotly_chart(fig3)
with st.expander("See explanation"):
     st.write("""
     Las 15 zonas con viajes mas largos sirven para estudiar las zonas donde la empresa de transporte desplegará principalmente sus servicios. Cabe destacar que las zonas aqui mostradas pertenecen principalmente al distrito de Queens y que tienen un promedio mayor a 20 km en sus viajes.
     """)
#Viajes con distancias mas cortas segun punto de partida
st.title("""Zonas donde la distancia promedio de los viajes es mas pequeña""")
fig4 = px.bar(df_distancias_mas_cortas, x = "zona", y = "distancia_promedio", title = "Zonas con viajes con distancias mas cortas en promedio", labels={"zona":"Zona de partida","distancia_promedio":"Promedio de distancias (Km) "},color='distancia_promedio',
            color_continuous_scale=px.colors.sequential.Electric)
fig4.update_xaxes(tickangle=45)
st.plotly_chart(fig4)
with st.expander("See explanation"):
     st.write("""
        Las 15 znas con viajes mas cortos no se encuentran especificamente concentradas en un solo distrito. Cabe resaltar que estas 15 zonas tienen un promedio menor a 3 km por viaje.
     """)
#Cantidad promedio de pasajeros por zona de partida
st.title("""Zonas donde los viajes llevan mas pasajeros""")
fig5 = px.bar(df_pasajeros_por_pu, x = "zona", y = "pasajeros", title = "Zonas donde los viajes llevan a mas pasajeros", labels={"zona":"Punto de origen","pasajeros":"Promedion de pasajeros"},color='pasajeros',
            color_continuous_scale=px.colors.sequential.Brwnyl)
fig5.update_xaxes(tickangle=45)
st.plotly_chart(fig5)
with st.expander("See explanation"):
     st.write("""
        El estudio de las zonas donde se tienen mas pasajeros nos ayuda a determinar las zonas en las que se necesitan mas unidades de transporte.        
     """)
#Recorridos mas frecuentes entre origen y destino
st.title("""Viajes que mas se repiten segun localidad de origen y destino""")
fig6 = px.bar(df_viajes_mas_frecuentes, x = "Origen-destino", y = "ocurrence", title = "Viajes que mas se repiten", labels={"Origen-destino":"Origen-Destino","ocurrence":"Ocurrencia"},color='ocurrence',
            color_continuous_scale=px.colors.sequential.Emrld)
fig6.update_xaxes(tickangle=30)
st.plotly_chart(fig6)
with st.expander("See explanation"):
     st.write("""
        El estudio de la ocurrencia de los viajes mas repetidos nos ayuda a concentrar los servicios de transporte en estas zonas         
     """)
