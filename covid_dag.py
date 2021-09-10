import requests
import json
import pandas as pd
import psycopg2 as pg
from datetime import date
from configparser import ConfigParser

config = ConfigParser()
config.read("pg_creds.cfg")

#############################################################################
# Extract / Transform
#############################################################################


def fetchDataToLocal():
    """
    Buscando dados de COVID da cidade de Nova Iorque através de uma API
    """
    
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)

    df = pd.DataFrame(json.loads(response.content))
    df = df.set_index("date_of_interest")
    
    df.to_csv("data/nyccovid_{}.csv".format(date.today().strftime("%Y%m%d")))
    

#############################################################################
# Load
#############################################################################


def sqlLoad():
    """
    Conectando no banco e importando os dados para o Postgres
    """
    #conexão com o banco de dados Postgres - Substitua pelas suas variáveis ^^
    try:
        dbconnect = pg.connect(
            database=config.get("postgres", "DATABASE"),
            user=config.get("postgres", "USERNAME"),
            password=config.get("postgres", "PASSWORD"),
            host=config.get("postgres", "HOST")
        )
    except Exception as error:
        print(error)
    
    cursor = dbconnect.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS covid_data (
            date DATE,
            case_count INT,
            hospitalized_count INT,
            death_count INT,
            PRIMARY KEY (date)
        );
        
        TRUNCATE TABLE covid_data;
    """
    )
    dbconnect.commit()
    
    with open("data/nyccovid_{}.csv".format(date.today().strftime("%Y%m%d"))) as f:
        next(f)
        for row in f:
            cursor.execute("""
                INSERT INTO covid_data
                VALUES ('{}', '{}', '{}', '{}')
            """.format(
            row.split(",")[0],
            row.split(",")[1],
            row.split(",")[2],
            row.split(",")[3])
            )
    dbconnect.commit()

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
default_args = {
    "owner": "airflow",
    "start_date": datetime.today() - timedelta(days=1)
              }
with DAG(
    "covid_nyc_data",
    default_args=default_args,
    schedule_interval = "0 1 * * *",
) as dag:
    
    fetchDataToLocal = PythonOperator(
        task_id="fetch_data_to_local",
        python_callable=fetchDataToLocal
    )
    
    sqlLoad = PythonOperator(
        task_id="sql_load",
        python_callable=sqlLoad
    )
    
    fetchDataToLocal >> sqlLoad
