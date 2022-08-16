
#streamlit run demanda_pred_streamlit.py 
import os
import streamlit as st

import pandas as pd
import numpy as np
import os
from datetime import datetime
from datetime import timedelta

import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go


#from scipy import stats
import statsmodels.api as sm
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.graphics.tsaplots import plot_pacf
from statsmodels.tsa.statespace.sarimax import SARIMAX


from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
#from skforecast.ForecasterAutoreg import ForecasterAutoreg

from joblib import dump, load
plt.style.use('ggplot')

primaryColor="#F63366"
backgroundColor="#FFFFFF"
secondaryBackgroundColor="#F0F2F6"
textColor="#262730"
font="sans serif"




st.set_page_config(page_title = 'Informe',layout = 'wide')
st.title("""Prediccion de demanda""")
#st.markdown("<h1 style='text-align: center; color:black;'> Predicción de demanda </h1>",unsafe_allow_html=True)
#st.markdown("<br></br>",unsafe_allow_html=True)

# INGESTA DE DATOS

os.chdir("./pages/assets/")


borough_1 = pd.read_csv('borough_1.csv',parse_dates=['PU_Datetime'],index_col='PU_Datetime').asfreq('H')
borough_2 = pd.read_csv('borough_2.csv',parse_dates=['PU_Datetime'],index_col='PU_Datetime').asfreq('H')
borough_3 = pd.read_csv('borough_3.csv',parse_dates=['PU_Datetime'],index_col='PU_Datetime').asfreq('H')
borough_4 = pd.read_csv('borough_4.csv',parse_dates=['PU_Datetime'],index_col='PU_Datetime').asfreq('H')
borough_5 = pd.read_csv('borough_5.csv',parse_dates=['PU_Datetime'],index_col='PU_Datetime').asfreq('H')
borough_6 = pd.read_csv('borough_6.csv',parse_dates=['PU_Datetime'],index_col='PU_Datetime').asfreq('H')

lista_boroughs = ['Bronx', 'Brooklyn', 'EWR', 'Manhattan', 'Queens', 'Staten Island']
lista_df_boroughs=[borough_1,borough_2,borough_3,borough_4,borough_5,borough_6]
dicc_boro = {lista_boroughs[i]:lista_df_boroughs[i] for i in range(len(lista_boroughs))}


with st.sidebar:
    selection = st.selectbox('Seleccione Borough:', lista_boroughs,index=3)
 
   
title= selection 
    
for clave, valor in dicc_boro.items():
    if clave == selection:
        boro = valor

with st.container():
        
    st.markdown(f"<h1 style='text-align:center; color:blue;'> {selection} </h1>",unsafe_allow_html=True)  

   
    # Gráfica inicial

    media_movil = boro['Demand'].rolling(window=48,min_periods=1).mean()

    fig = px.line(boro, x=boro.index, y=boro.Demand)
    fig.add_trace(go.Scatter(x=media_movil.index, y=media_movil.values, name='Trend',
                             line=dict(color='firebrick', width=4)))

    fig.update_xaxes(tickformat="%b\n%d")
    fig.update_xaxes(rangeslider_visible=True)
    fig.update_layout(title={
            'text': 'Demanda de taxis amarillos',
            'y':0.9,
            'x':0.5,
            'font_size':25,
            'xanchor': 'center',
            'yanchor': 'top'},
            xaxis_title="Pick up datetime",)

    st.plotly_chart(fig)
    
  
    
with st.expander("Ver descomposición de la serie y correlaciones"):
    fig1, fig2 = st.columns(2)
    
    #Gráfica de  descomposición de la serie 
    with fig1:
        
        st.markdown(f"<h2 style='text-align:center; color:blue;'> Descomposición de la serie </h2>",unsafe_allow_html=True)
        fig, axes = plt.subplots(ncols=2, nrows=4, figsize=(11,6))
        fig.suptitle('{}\n\n\n\n'.format(selection), fontsize = 15, fontweight = "bold")
        by_hour = seasonal_decompose(boro.Demand, model='additive')

        by_hour.observed.plot(ax=axes[0,0], legend=False,xlabel='')
        axes[0,0].set_ylabel('Observed')
        by_hour.trend.plot(ax=axes[1,0], legend=False,xlabel='')
        axes[1,0].set_ylabel('Trend')
        by_hour.seasonal.plot(ax=axes[2,0], legend=False,xlabel='')
        axes[2,0].set_ylabel('Seasonal')
        by_hour.resid.plot(ax=axes[3,0], legend=False,xlabel='')
        axes[3,0].set_ylabel('Residual')
        fig.tight_layout()
        axes[0,0].set_title('Hour')
          

        boro_day = boro.asfreq('D')
        boro_day.Demand.fillna(0,inplace=True)
        by_month = seasonal_decompose(boro_day.Demand, model='additive')
         
        by_month.observed.plot(ax=axes[0,1], legend=False,xlabel='')
        axes[0,1].set_ylabel('Observed')
        by_month.trend.plot(ax=axes[1,1], legend=False,xlabel='')
        axes[1,1].set_ylabel('Trend')
        by_month.seasonal.plot(ax=axes[2,1], legend=False,xlabel='')
        axes[2,1].set_ylabel('Seasonal')
        by_month.resid.plot(ax=axes[3,1], legend=False,xlabel='')
        axes[3,1].set_ylabel('Residual')

        axes[0,1].set_title('Day')
        fig.tight_layout()
        
        st.pyplot(fig)
        
    # Correlaciones
    with fig2:
        st.markdown(f"<h2 style='text-align:center; color:blue;'> Correlaciones </h2>",unsafe_allow_html=True)
                

        lags_plots =range(1,49)
        fig = plt.figure(figsize=(12,7))
        ax1 = plt.subplot2grid((2,2), (0,0))
        ax2 = plt.subplot2grid((2,2), (0,1))
        ax3 = plt.subplot2grid((2,2), (1,0),colspan=2)
        plot_acf(boro.Demand, lags=lags_plots, zero=False, ax=ax1)
        plot_pacf(boro.Demand, lags=lags_plots, zero=False, ax=ax2)
        sns.distplot(boro.Demand, ax=ax3)
        st.pyplot(fig)
        
# IMPLEMENTACIÓN DEL MODELO
with st.container():
    st.markdown(f"<h1 style='text-align:center; color:blue;'> Evaluación del modelo </h1>",unsafe_allow_html=True)    
     
    # Se separan datos de entrenamiento y test
    split_date = pd.Timestamp('2022-03-21')
    boro['Precip'].fillna(boro['Precip'].median(),inplace=True)
    boro['Temp'].fillna(boro['Temp'].median(),inplace=True)
    boro.dropna(inplace=True)
    train = boro.loc[:split_date]
    test =  boro.loc[split_date:]



    train_exog = train.loc[:,['Dayofmonth', 'Dayofweek', 'Hour', 'Precip', 'Temp']]
    test_exog = test.loc[:,['Dayofmonth', 'Dayofweek', 'Hour', 'Precip', 'Temp']]

    # Forecast 
    res = load('sarimax_{}.py'.format(selection))
    train_forecast= res.forecast(steps= train.Demand.size,exog=train_exog)
    test_forecast= res.forecast(steps= test.Demand.size,exog=test_exog)

    

    # Grafica de prueba del modelo 
    fig = px.line(boro, x=boro.index, y=boro.Demand)
    fig.add_vrect(x0=split_date,x1=test.index[-1], line_width=2, line_dash="dash", line_color="red",fillcolor="red", opacity=0.2,annotation_text="Test", annotation_position="top left",
                  annotation=dict(font_size=20, font_family="Times New Roman"))
    fig.add_vrect(x0=train.index[0],x1=split_date, line_width=2, line_dash="dash", line_color="green",fillcolor="green", opacity=0.2,annotation_text="Train", annotation_position="top left",
                  annotation=dict(font_size=20, font_family="Times New Roman"))


    fig.add_trace(go.Scatter(x=train.index, y=train_forecast, name='Train Pred', 
                             line=dict(color='green', width=4)))
    fig.add_trace(go.Scatter(x=test.index, y=test_forecast, name='Test Pred',
                             line=dict(color='firebrick', width=4)))

    fig.update_layout(title={
            'text': selection,
            'x':0.5,
            'font_size':25,
            'xanchor': 'center',
            'yanchor': 'top'},
            xaxis_title="Pick up datetime")

    fig.update_xaxes(tickformat="%b\n%d")
    fig.update_xaxes(rangeslider_visible=True)
    st.plotly_chart(fig)
    
    with st.expander("Métricas"):
    # Métricas de datos de entrenamiento y test
        def metrics_sarima(train_data,test_data,train_forecast,test_forecast):
            st.metric(label = 'Train Mean Absolute Error: ', value =mean_absolute_error(train.Demand , train_forecast))
            st.metric(label = 'Train Root Mean Squared Error: ',value = np.sqrt(mean_squared_error(train.Demand , train_forecast)))
            st.metric(label = 'Test Mean Absolute Error: ', value = mean_absolute_error(test.Demand, test_forecast))
            st.metric(label = 'Test Root Mean Squared Error: ', value = np.sqrt(mean_squared_error(test.Demand, test_forecast)))

        metrics_sarima(train,test,train_forecast, test_forecast) 

        
with st.expander("Ver análisis de residuos"):
    val= pd.DataFrame()
    val['train'] = train.Demand
    val['Pred']= train_forecast
    val['error'] = val['Pred'] -val['train'] 

    fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(12, 12))
    axes[0, 0].scatter(val['train'], val['Pred'], edgecolors=(0, 0, 0), alpha = 0.4)
    axes[0, 0].plot([val['train'].min(), val['train'].max()], [val['train'].min(), val['train'].max()], 'k--', color = 'black', lw=2)
    axes[0, 0].set_title('Valor predicho vs valor real', fontsize = 10, fontweight = "bold")
    axes[0, 0].set_xlabel('Real')
    axes[0, 0].set_ylabel('Predicción')
    axes[0 ,0].set_xlim(-1,val.values.max())
    axes[0 ,0].set_ylim(0,val.values.max())


    axes[0, 1].scatter(list(range(len(val['train']))), val['error'], edgecolors=(0, 0, 0), alpha = 0.4)
    axes[0, 1].axhline(y = 0, linestyle = '--', color = 'black', lw=2)
    axes[0, 1].set_title('Residuos del modelo', fontsize = 10, fontweight = "bold")
    axes[0, 1].set_xlabel('id')
    axes[0, 1].set_ylabel('Residuo')

    sns.histplot(
        data    = val['error'],
        stat    = "density",
        kde     = True,
        line_kws= {'linewidth': 1},
        color   = "firebrick",
        alpha   = 0.3,
        ax      = axes[1, 0]
    )
    axes[1, 0].set_title('Distribución residuos del modelo', fontsize = 10,fontweight = "bold")
    axes[1, 0].set_xlabel("Residuo")
    axes[1, 0].tick_params(labelsize = 7)


    sm.qqplot(
        val['error'],
        fit   = True,
        line  = 'q',
        ax    = axes[1, 1], 
        color = 'firebrick',
        alpha = 0.4,
        lw    = 2
    )
    axes[1, 1].set_title('Q-Q residuos del modelo', fontsize = 10, fontweight = "bold")
    axes[1, 1].tick_params(labelsize = 7)
    axes[1 ,1].set_xlim(0,3)
    axes[1 ,1].set_ylim(0,2)



    axes[2, 0].scatter(val['Pred'], val['error'], edgecolors=(0, 0, 0), alpha = 0.4)
    axes[2, 0].axhline(y = 0, linestyle = '--', color = 'black', lw=2)
    axes[2, 0].set_title('Residuos del modelo vs predicción', fontsize = 10, fontweight = "bold")
    axes[2, 0].set_xlabel('Predicción')
    axes[2, 0].set_ylabel('Residuo')
    axes[2, 0].tick_params(labelsize = 7)

    # Se eliminan los axes vacíos
    fig.delaxes(axes[2,1])

    fig.tight_layout()
    plt.subplots_adjust(top=0.9)
    fig.suptitle('Diagnóstico residuos', fontsize = 14, fontweight = "bold")
    st.pyplot(fig)
    
 # PREDICCIÓN 
with st.container():
    st.markdown(f"<h1 style='text-align:center; color:blue;'> Predicción </h1>",unsafe_allow_html=True)  
    
    
    horizonte_predict = st.number_input(label='Horizonte de predicción (días):',value=7)
    #horizonte_predict = 7
    
    Fecha=pd.date_range(test.index[-1], periods=horizonte_predict+1)
    new_data=pd.DataFrame(Fecha,columns=['Fecha']).set_index('Fecha').asfreq('H')
    new_data=new_data[:-1]

    new_data['Dayofmonth']=  new_data.index.map(lambda row: row.day)
    new_data['Dayofweek']= new_data.index.map(lambda row: row.dayofweek)
    new_data['Hour']=  new_data.index.map(lambda row: row.hour)
    new_data['Precip'] = [0,2,5,3,8,5,6]*24
    new_data['Temp'] = [0,2,5,3,8,5,6]*24

    new_forecast= res.forecast(steps= new_data.shape[0], exog=new_data.values)
    conf_interval= res.get_forecast(steps=len(new_forecast),exog=new_data.values,dynamic=False).conf_int()
    new_forecast[new_forecast<0]=0
    minim=pd.Series(conf_interval[:,0])
    minim=minim.where(minim>0,0)
    pred=pd.Series(new_forecast)
    maxim= pd.Series(conf_interval[:,1])
    intervalo = pd.DataFrame({'min':minim, 'max':maxim})

    fig = px.line(boro, x=boro.index, y=boro.Demand)
    fig.add_trace(go.Scatter(x=new_data.index, y=new_forecast, name='Forecast', 
                             line=dict(color='green', width=3)))
    fig.add_trace(go.Scatter(x=new_data.index, y=intervalo['min'],mode='lines', line_color='indigo',name='Lim inferior'))

    fig.add_trace(go.Scatter(x=new_data.index, y=intervalo['max'], name='Lim superior',fill='tonexty' ,mode='lines', line_color='indigo'))

    fig.update_xaxes(tickformat="%b\n%d")
    fig.update_xaxes(rangeslider_visible=True)

    fig.update_layout(title={
            'text': 'Predicción',
            'x':0.5,
            'font_size':25,
            'xanchor': 'center',
            'yanchor': 'top'},
            xaxis_title="Pick up datetime")

    st.plotly_chart(fig)
    

with st.container():
    st.markdown(f"<h1 style='text-align:center; color:blue;'> Predicción de demanda por Borough</h1>",unsafe_allow_html=True)      
    
    fecha_inicio_pred= st.date_input( "Ingrese fecha y hora de predicción: ",value = pd.to_datetime('01/04/2022 00:00', format='%d/%m/%Y %H:%M'), min_value = pd.to_datetime('01/04/2022 00:00', format='%d/%m/%Y %H:%M'),max_value= pd.to_datetime('01/05/2022 00:00', format='%d/%m/%Y %H:%M'))
    fecha_inicio_pred = pd.Timestamp(fecha_inicio_pred.strftime("%Y-%m-%d %H:%M:%S"))
    
    hora = st.number_input(label='Hora:',value=15)
    fecha_inicio_pred = fecha_inicio_pred+pd.Timedelta(int(hora), unit='h')
    
    fecha_predict= pd.Timestamp('2022-04-01 00:00:00')
    def convert_to_hours(delta):
        total_seconds = delta.total_seconds()
        hours = str(int(total_seconds // 3600)).zfill(2)
        minutes = str(int((total_seconds % 3600) // 60)).zfill(2)
        seconds = str(int(total_seconds % 60)).zfill(2)
        return int(hours)



    
    resultado = pd.DataFrame(columns=['Demanda'])

    for i in lista_boroughs:

      

      forecaster= load('sarimax_{}.py'.format(i))
      for clave, valor in dicc_boro.items():
         if clave == i:
            boro = valor



      #fecha_inicio_pred=pd.Timestamp('2022-04-01 00:00:00' )

      horizonte_predict = fecha_predict-fecha_inicio_pred
      Fecha=pd.date_range(start=fecha_inicio_pred, end=fecha_predict, freq='H')
      new_data=pd.DataFrame(Fecha,columns=['Fecha']).set_index('Fecha').asfreq('H')
      new_data=new_data[:-1]
      new_data['Dayofmonth']=  new_data.index.map(lambda row: row.day)
      new_data['Dayofweek']= new_data.index.map(lambda row: row.dayofweek)
      new_data['Hour']=  new_data.index.map(lambda row: row.hour)
      new_data['Precip'] = [boro.Precip.median()]*convert_to_hours(horizonte_predict)
      new_data['Temp'] = [boro.Temp.median()]*convert_to_hours(horizonte_predict)
      
      new_forecast = forecaster.predict(steps= new_data.shape[0], exog=new_data)
      
      resultado.loc[i] =  int(np.floor(new_forecast[-1]))
      resultado.Demanda[resultado.Demanda<0]=0

    st.dataframe(resultado)