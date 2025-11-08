from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

from extraer_datos import extraer_datos
from transformar_datos import transformar_datos
from cargar_datos import cargar_datos

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'mi_primer_etl',
    default_args=default_args,
    description='Un ETL simple',
    schedule='@daily',
    catchup=False,
    tags=['etl'],
) as dag:

    # Definir rutas de archivos temporales
    ruta_base = '/home/maguilar/airflow-project/data'
    archivo_extraido = f'{ruta_base}/datos_extraidos.csv'
    archivo_transformado = f'{ruta_base}/datos_transformados.csv'
    archivo_salida = f'{ruta_base}/datos_salida.csv'

    # Tarea 1: Extraer datos
    t1 = PythonOperator(
        task_id='extraer_datos',
        python_callable=extraer_datos,
        op_kwargs={'archivo_salida': archivo_extraido}
    )
    
    # Tarea 2: Transformar datos  
    t2 = PythonOperator(
        task_id='transformar_datos',
        python_callable=transformar_datos,
        op_kwargs={
            'archivo_entrada': archivo_extraido,
            'archivo_salida': archivo_transformado
        }
    )
    
    # Tarea 3: Cargar datos
    t3 = PythonOperator(
        task_id='cargar_datos',
        python_callable=cargar_datos,
        op_kwargs={
            'archivo_entrada': archivo_transformado,
            'archivo_salida': archivo_salida
        }
    )
    
    t1 >> t2 >> t3