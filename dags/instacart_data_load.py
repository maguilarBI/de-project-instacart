from airflow.decorators import dag, task
from pendulum import timezone
from datetime import datetime, timedelta
import pandas as pd
import os
from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix

# Configuración
RAW_DATA_PATH = "/home/maguilar/airflow-project/data/instacart_dataset/"
SAMPLE_DATA_PATH = "/home/maguilar/airflow-project/data/instacart_sample/"
SAMPLE_SIZE = 0.01  # 1% de muestra
CONTAINER_NAME = "datalake"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='instacart_initial_load',
    default_args=default_args,
    description='Carga inicial de datos Instacart muestreados para desarrollo',
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 12 * * 1", # Runs every monday at 12:00 local time (GMT-5)
    catchup=False,
    tags=['instacart', 'data_load', 'dev'],
)
def instacart_data_load_dag():

    @task
    def get_orders_sample():
        """
        Obtiene muestra aleatoria de orders incluyendo 'prior' y 'train'
        Retorna metadata en lugar de la lista completa para evitar logs largos
        """
        orders_df = pd.read_csv(os.path.join(RAW_DATA_PATH, 'orders.csv'))
        
        # Filtrar solo prior y train (excluir test para desarrollo)
        filtered_orders = orders_df[orders_df['eval_set'].isin(['prior', 'train'])]
        
        # Tomar muestra aleatoria de usuarios para mantener integridad referencial
        unique_users = filtered_orders['user_id'].unique()
        sample_users = pd.Series(unique_users).sample(frac=SAMPLE_SIZE, random_state=42)
        
        # Obtener todos los pedidos de los usuarios muestreados
        orders_sample = filtered_orders[filtered_orders['user_id'].isin(sample_users)]
        
        # Guardar muestra de orders
        os.makedirs(SAMPLE_DATA_PATH, exist_ok=True)
        orders_sample.to_csv(os.path.join(SAMPLE_DATA_PATH, 'orders_sample.csv'), index=False)
        
        print(f"📊 Muestra creada: {len(orders_sample)} órdenes de {len(sample_users)} usuarios")
        
        # Guardar order_ids en archivo temporal para evitar logs largos
        order_ids = orders_sample['order_id'].tolist()
        order_ids_path = os.path.join(SAMPLE_DATA_PATH, 'temp_order_ids.txt')
        with open(order_ids_path, 'w') as f:
            f.write(','.join(map(str, order_ids)))
        
        # Retornar solo metadata, no la lista completa
        return {
            "total_orders": len(orders_sample),
            "total_users": len(sample_users),
            "order_ids_file": order_ids_path,
            "message": f"Muestra de {len(orders_sample)} órdenes creada exitosamente"
        }

    @task
    def load_prior_products(get_orders_sample_result):
        """Carga order_products__prior filtrado por los order_ids de la muestra"""
        # Leer order_ids desde archivo en lugar de XCom
        order_ids_path = get_orders_sample_result["order_ids_file"]
        with open(order_ids_path, 'r') as f:
            order_ids_str = f.read()
            order_ids = list(map(int, order_ids_str.split(',')))
        
        print(f"📦 Procesando {len(order_ids)} order_ids para prior products")
        
        prior_df = pd.read_csv(os.path.join(RAW_DATA_PATH, 'order_products__prior.csv'))
        
        # Filtrar solo los order_ids de nuestra muestra
        prior_sample = prior_df[prior_df['order_id'].isin(order_ids)]
        
        prior_sample.to_csv(os.path.join(SAMPLE_DATA_PATH, 'order_products__prior_sample.csv'), index=False)
        
        print(f"📦 Prior products: {len(prior_sample)} registros")
        return f"Loaded {len(prior_sample)} prior products records"

    @task
    def load_train_products(get_orders_sample_result):
        """Carga order_products__train filtrado por los order_ids de la muestra"""
        # Leer order_ids desde archivo en lugar de XCom
        order_ids_path = get_orders_sample_result["order_ids_file"]
        with open(order_ids_path, 'r') as f:
            order_ids_str = f.read()
            order_ids = list(map(int, order_ids_str.split(',')))
        
        print(f"📦 Procesando {len(order_ids)} order_ids para train products")
        
        train_df = pd.read_csv(os.path.join(RAW_DATA_PATH, 'order_products__train.csv'))
        
        # Filtrar solo los order_ids de nuestra muestra
        train_sample = train_df[train_df['order_id'].isin(order_ids)]
        
        train_sample.to_csv(os.path.join(SAMPLE_DATA_PATH, 'order_products__train_sample.csv'), index=False)
        
        print(f"📦 Train products: {len(train_sample)} registros")
        return f"Loaded {len(train_sample)} train products records"

    @task
    def load_train_products(order_ids):
        """Carga order_products__train filtrado por los order_ids de la muestra"""
        train_df = pd.read_csv(os.path.join(RAW_DATA_PATH, 'order_products__train.csv'))
        
        # Filtrar solo los order_ids de nuestra muestra
        train_sample = train_df[train_df['order_id'].isin(order_ids)]
        
        train_sample.to_csv(os.path.join(SAMPLE_DATA_PATH, 'order_products__train_sample.csv'), index=False)
        
        print(f"📦 Train products: {len(train_sample)} registros")
        return f"Loaded {len(train_sample)} train products records"

    @task
    def load_master_tables():
        """Carga tablas maestras completas en paralelo"""
        master_tables = ['products', 'aisles', 'departments']
        
        for table in master_tables:
            df = pd.read_csv(os.path.join(RAW_DATA_PATH, f'{table}.csv'))
            df.to_csv(os.path.join(SAMPLE_DATA_PATH, f'{table}.csv'), index=False)
            print(f"📁 {table}: {len(df)} registros")
        
        return "Master tables loaded completely"

    @task
    def upload_to_azure():
        """Sube todos los archivos muestreados a Azure"""
        files_to_upload = [
            'orders_sample.csv',
            'order_products__prior_sample.csv', 
            'order_products__train_sample.csv',
            'products.csv',
            'aisles.csv',
            'departments.csv'
        ]
        
        uploaded_count = 0
        for file_name in files_to_upload:
            local_path = os.path.join(SAMPLE_DATA_PATH, file_name)
            # CORRECCIÓN: Agregar la carpeta airflow/G2 en la ruta del blob
            blob_name = f"raw/airflow/G2/{file_name}"
            
            if os.path.exists(local_path):
                # Agregar fecha al nombre del archivo
                dated_blob_name = add_date_suffix(blob_name)
                
                success = upload_to_adls(
                    local_file_path=local_path,
                    container_name=CONTAINER_NAME,
                    blob_name=dated_blob_name
                )
                
                if success:
                    uploaded_count += 1
                    print(f"✅ Subido: {file_name} → {dated_blob_name}")
                else:
                    print(f"❌ Falló: {file_name}")
            else:
                print(f"⚠️  No encontrado: {local_path}")
        
        # Limpiar archivo temporal de order_ids
        temp_ids_file = os.path.join(SAMPLE_DATA_PATH, 'temp_order_ids.txt')
        if os.path.exists(temp_ids_file):
            os.remove(temp_ids_file)
            print("🧹 Archivo temporal de order_ids eliminado")        
        
        return f"Uploaded {uploaded_count}/{len(files_to_upload)} files to Azure"

    # Orquestación del DAG
    order_ids = get_orders_sample()
    
    # Ejecutar en paralelo
    prior_task = load_prior_products(order_ids)
    train_task = load_train_products(order_ids) 
    masters_task = load_master_tables()
    
    # Upload después de que todas las cargas estén completas
    upload_task = upload_to_azure()
    
    # Dependencias
    order_ids >> [prior_task, train_task, masters_task] >> upload_task

# Instanciar el DAG
dag = instacart_data_load_dag()