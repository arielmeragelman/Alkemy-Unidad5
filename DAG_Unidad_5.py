# import the libraries
import logging
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
import pandas as pd

       




# defining DAG arguments

default_args = {
 'owner': 'Alkemy_Prisma',
 'start_date': days_ago(0),
 'email': ['some@somemail.com'],
 'email_on_failure': False,
 'email_on_retry': False,
 'retries': 1,
 'retry_delay': timedelta(minutes=5),
}


dag = DAG(
 'DAG_Unidad_5',
 default_args=default_args,
 description='My first DAG',
 schedule_interval=timedelta(days=1),
)

# Tasks


@dag.task(task_id = "read_top10")
def read_top10():
    
    # ----- Completar logging ------
    # Definimos el logger personalizado

    logger_personal = logging.getLogger("custom_log")
    nivel= logging.getLevelName("INFO")
    logger_personal.setLevel(nivel)
    formato = logging.Formatter(" %(asctime)s : %(levelname)s : %(message)s  ")
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formato)

    file_handler = logging.FileHandler(filename="/opt/airflow/logs/log_personal.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formato)


    logger_personal.addHandler(stream_handler)
    logger_personal.addHandler(file_handler)

    # -----Fin Completar logging ------




    # Read CSV from web
    url = "http://winterolympicsmedals.com/medals.csv"
    
    try:

        df = pd.read_csv(url)
        logger_personal.info("Archivo descargado correctamente")
        # Get top 10 countries with most medals
        top_countries = df.NOC.value_counts().sort_values(ascending=False).head(10)
        
        # Convert pandas series to data frame
        to_countries_df = top_countries.to_frame()
        
        # Save data frame in Excel format - Completar tu propia ubicación para guardar el archivo de salida
        to_countries_df.to_excel('/opt/airflow/output/top10_medals_by_country.xlsx')

        #Logging message INFO Success --- Completar
        logger_personal.info("Archivo procesado correctamente")
    except Exception as e:
        #Logging message ERROR Fail --- Completar
        logger_personal.error("No se pudo descargar o procesar el archivo")
        logger_personal.error(e)

 

# task pipeline
read_top10()
