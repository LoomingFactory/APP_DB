import streamlit as st
from streamlit_option_menu import option_menu

from datetime import datetime
import pandas as pd
import pymongo

def dt_to_ts(data_str):
    dt = datetime.strptime(data_str, '%Y-%m-%d')
    ts=int(round(dt.timestamp()))*1000
    return ts

def ts_to_dt(temps):
  dt_object = datetime.fromtimestamp(temps/1000)
  data = dt_object.strftime("%Y-%m-%d")
  return data


def ax_despres_login():
    if usuari != st.secrets["login"]["usuari"] or contra != st.secrets["login"]["contra"]:
        st.error('Usuari i/o contrasenya erronies')
    elif usuari == st.secrets["login"]["usuari"] and contra == st.secrets["login"]["contra"]:
        #st.balloons()
        st.session_state['pagina'] = 'dins_app'


# li dic que comenci amb la pagina de login
if 'pagina' not in st.session_state:
	st.session_state['pagina'] = 'login'

################################################    
#################### LOG IN ####################
################################################

if st.session_state['pagina'] == 'login':
    st.title("HAAS DB")
    st.write("@CIM UPC")

    with st.sidebar:
        usuari = st.text_input('Usuari: ')
        contra = st.text_input('Contrasenya: ',type = 'password')
        boto_login = st.button('login', on_click = ax_despres_login) #aixi refresco la pagina i ja estare a dins de la app

    
##################################################
#################### DINS APP ####################
##################################################

elif st.session_state['pagina'] == 'dins_app':

    with st.sidebar:

        # icones a https://icons.getbootstrap.com/
        opcio_dates = option_menu(menu_title = 'Buscar per dates', options= ["Interval", "Relatiu"] , icons= ["calendar-week", "stopwatch"], menu_icon= "calendar-plus", orientation= "horizontal")

        if opcio_dates == "Interval":
            di_dtt = st.date_input("Data inici") 
            df_dtt= st.date_input("Data fi") 
            di = dt_to_ts(str(di_dtt))
            df = dt_to_ts(str(df_dtt))

            lower_than= df
            greater_than = di

        elif opcio_dates == "Relatiu":

            op_timestamp = [604800000, 1209600000, 2592000000, 5184000000]
            op = ['1 setmana','2 setmanes', '1 mes', '2 mesos']
            opcions = st.selectbox('Des de fa: ', op)

            dara_dtt = datetime.now()
            dara = round(datetime.timestamp(dara_dtt)*1000)

            drel = dara - op_timestamp[op.index(opcions)]

            drel_dtt = ts_to_dt(drel)
            st.write('   ', drel_dtt)

            lower_than=  dara #1643197403000 #
            greater_than = drel #1643194196000  #

            st.write('   ')
            st.write('   ')


        st.write('   ')
        st.write('   ')
        st.write('   ')
        st.write('   ')
        st.write('   ')
        MOSTRAR_DB = st.button('Mostrar DB')


    ############################### CONNECTO DB MONGO ##################################
    st.title('Resultats de la cerca')
    st.write("Aquí es mostraran la base de dades de EINES i ENERGIES per l'interval seleccionat, ademés de la DB de ENERGIES en el període restringit a les dades de EINES anteriors")

    if MOSTRAR_DB:
        # --------------- CONNECTO MONGO: Uses st.experimental_singleton to only run once ---------------
        @st.experimental_singleton
        def init_connection():
            return pymongo.MongoClient(st.secrets["mongo"]["uri_publica"]) #"mongodb://xxxxxxxxxx:yyyyy/" on xxx... = ip públic del ordi al q es connecta // yy = port on esta el mongodb

        try:
            client = init_connection()
	    st.info ("s'ha connectat correctament a la base de dades")
        except pymongo.errors.ServerSelectionTimeoutError as errorTiempo:
            st.error("Temps de connexio al MongoDB excedit. Temps: "+errorTiempo)
        except pymongo.errors.ConnectionFailure as errorConexion:
            st.error("Error al connectarse a MongoDB. Error: "+errorConexion)
        except pymongo.errors.ConfigurationError as errorConfig:
            st.error("Error de configuracio de MongoDB. Error: "+errorConfig)
        except pymongo.errors.OperationFailure as errorOperacio:
            st.error("Error d'operacio de MongoDB. Error: "+errorOperacio)
        except pymongo.errors.InvalidURI as errorURI:
            st.error("Error de URI de MongoDB. Error: "+errorURI)
        except pymongo.errors.PyMongoError as errorPyMongo:
            st.error("Error de PyMongo. Error: "+errorPyMongo)


        ########################################## EINES ##########################################
        st.title('1. DB EINES')
        @st.experimental_memo(ttl=6000)
        def get_data_Eines1(lower_than,greater_than):
            db = client.eines.Eines1
            buscar = db.find({'TIMESTAMP': {'$lt':lower_than , '$gte': greater_than}})
            items = list(buscar)  # make hashable for st.experimental_memo
            df = pd.DataFrame(items)
            if not df.empty:
                df.drop('_id', axis=1, inplace=True)
                cols = df.columns.tolist()
                cols = cols[-1:] + cols[:-1]
                df = df[cols] 
            return df


        df_eines = get_data_Eines1(lower_than,greater_than)
        st.write('Rows :', df_eines.shape[0])
        st.dataframe(df_eines.head())

        if not df_eines.empty:
            max_timestamp = int(df_eines['TIMESTAMP'].max())
            min_timestamp = int(df_eines['TIMESTAMP'].min())

            descargable = df_eines.to_csv().encode('utf-8')
            st.download_button("Descarregar DB EINES",descargable,"resultao.csv","text/csv",key='download-csv')

        guardar = False
        if guardar:
            df_eines.to_csv('D:\DOCUMENTS\CIM\jeje\Eines.csv', header=True, index=False)

        ########################################## ENERGIES ##########################################

        st.title('2. DB ENERGIES')
        @st.experimental_memo(ttl=6000)
        def get_data_Energia1(lower_than,greater_than):
            db = client.eines.Energia1
            buscar = db.find({'TIMESTAMP': {'$lt':lower_than , '$gte': greater_than}})
            items = list(buscar)  # make hashable for st.experimental_memo
            df = pd.DataFrame(items)
            if not df.empty:
                df.drop('_id', axis=1, inplace=True)
            return df

        df_energia = get_data_Energia1(lower_than,greater_than)
        st.write('Rows :', df_energia.shape[0])
        st.dataframe(df_energia.head())

        if not df_energia.empty:
            descargable = df_energia.to_csv().encode('utf-8')
            st.download_button("Descarregar DB ENERGIES",descargable,"resultao.csv","text/csv",key='download-csv')


        ########################################## ENERGIES pertanyent a EINES ##########################################

        st.title('3. DB ENERGIES pertanyent a eines')
        if not df_eines.empty: 
        
            if not df_energia.empty:

                @st.experimental_memo(ttl=6000)
                def get_data_Energia1_pertanyent_Eines1(max_timestamp,min_timestamp):
                    db = client.eines.Energia1
                    buscar = db.find({'TIMESTAMP': {'$lt':max_timestamp , '$gte': min_timestamp}})
                    items = list(buscar)  # make hashable for st.experimental_memo
                    df = pd.DataFrame(items)
                    if not df.empty:
                        df.drop('_id', axis=1, inplace=True)
                    return df


                df_energia_de_eines = get_data_Energia1_pertanyent_Eines1(max_timestamp,min_timestamp)
                st.write('Rows :', df_energia_de_eines.shape[0])
                st.dataframe(df_energia_de_eines.head())

                if not df_energia_de_eines.empty:
                    descargable = df_energia_de_eines.to_csv().encode('utf-8')
                    st.download_button("Descarregar DB ENERGIES d'eines",descargable,"resultao.csv","text/csv",key='download-csv')
                
            else:
                st.write("No hi ha dades d'energies")
        
        else:
            if df_energia.empty:
                st.write("No hi ha dades d'eines ni d'energies")
            else:
                st.write("No hi ha dades d'eines")
