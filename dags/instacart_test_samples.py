from airflow.decorators import dag, task
from pendulum import timezone
from datetime import datetime, timedelta
import pandas as pd
import os
import shutil

# Configuración
RAW_DATA_PATH = "/home/maguilar/airflow-project/data/instacart_dataset/"
SAMPLE_DATA_PATH = "/home/maguilar/airflow-project/data/instacart_sample/"
SAMPLE_SIZE = 0.01  # 1% de muestra

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5),
}

@dag(
    dag_id='instacart_sample_creation',
    default_args=default_args,
    description='Crea muestras de datos Instacart para desarrollo sin subir a Azure',
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 12 * * 1", # Runs every monday at 12:00 local time (GMT-5)
    catchup=False,
    tags=['instacart', 'sample', 'dev'],
)
def instacart_sample_creation_dag():

    @task
    def cleanup_previous_samples():
        """Limpia muestras anteriores antes de crear nuevas"""
        if os.path.exists(SAMPLE_DATA_PATH):
            shutil.rmtree(SAMPLE_DATA_PATH)
            print(f"🧹 Directorio limpiado: {SAMPLE_DATA_PATH}")
        os.makedirs(SAMPLE_DATA_PATH, exist_ok=True)
        return "Cleanup completed"

    @task
    def get_orders_sample():
        """
        Obtiene muestra aleatoria de orders incluyendo 'prior' y 'train'
        Retorna solo metadata para evitar imprimir listas largas en logs
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
        orders_sample.to_csv(os.path.join(SAMPLE_DATA_PATH, 'orders_sample.csv'), index=False)
        
        print(f"📊 Muestra creada: {len(orders_sample)} órdenes de {len(sample_users)} usuarios")
        
        # En lugar de retornar todos los order_ids, retornar solo metadata
        # y guardar los order_ids en un archivo temporal
        order_ids = orders_sample['order_id'].tolist()
        
        # Guardar order_ids en archivo para evitar XCom largo
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
        # Leer order_ids desde archivo
        order_ids_path = get_orders_sample_result["order_ids_file"]
        with open(order_ids_path, 'r') as f:
            order_ids_str = f.read()
            order_ids = list(map(int, order_ids_str.split(',')))
        
        print(f"📦 Procesando {len(order_ids)} order_ids para prior products")
        
        # Leer y filtrar datos
        prior_df = pd.read_csv(os.path.join(RAW_DATA_PATH, 'order_products__prior.csv'))
        prior_sample = prior_df[prior_df['order_id'].isin(order_ids)]
        
        prior_sample.to_csv(os.path.join(SAMPLE_DATA_PATH, 'order_products__prior_sample.csv'), index=False)
        
        print(f"📦 Prior products: {len(prior_sample)} registros")
        return f"Loaded {len(prior_sample)} prior products records"

    @task
    def load_train_products(get_orders_sample_result):
        """Carga order_products__train filtrado por los order_ids de la muestra"""
        # Leer order_ids desde archivo
        order_ids_path = get_orders_sample_result["order_ids_file"]
        with open(order_ids_path, 'r') as f:
            order_ids_str = f.read()
            order_ids = list(map(int, order_ids_str.split(',')))
        
        print(f"📦 Procesando {len(order_ids)} order_ids para train products")
        
        # Leer y filtrar datos
        train_df = pd.read_csv(os.path.join(RAW_DATA_PATH, 'order_products__train.csv'))
        train_sample = train_df[train_df['order_id'].isin(order_ids)]
        
        train_sample.to_csv(os.path.join(SAMPLE_DATA_PATH, 'order_products__train_sample.csv'), index=False)
        
        print(f"📦 Train products: {len(train_sample)} registros")
        return f"Loaded {len(train_sample)} train products records"

    @task
    def load_master_tables():
        """Carga tablas maestras completas"""
        master_tables = ['products', 'aisles', 'departments']
        
        for table in master_tables:
            df = pd.read_csv(os.path.join(RAW_DATA_PATH, f'{table}.csv'))
            df.to_csv(os.path.join(SAMPLE_DATA_PATH, f'{table}.csv'), index=False)
            print(f"📁 {table}: {len(df)} registros")
        
        return "Master tables loaded completely"

    @task
    def validate_samples():
        """Valida que todos los archivos de muestra se crearon correctamente"""
        expected_files = [
            'orders_sample.csv',
            'order_products__prior_sample.csv', 
            'order_products__train_sample.csv',
            'products.csv',
            'aisles.csv',
            'departments.csv'
        ]
        
        missing_files = []
        for file_name in expected_files:
            file_path = os.path.join(SAMPLE_DATA_PATH, file_name)
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path) / 1024  # KB
                df = pd.read_csv(file_path)
                print(f"✅ {file_name}: {file_size:.1f} KB, {len(df)} registros")
            else:
                missing_files.append(file_name)
                print(f"❌ {file_name}: NO ENCONTRADO")
        
        # Limpiar archivo temporal de order_ids
        temp_ids_file = os.path.join(SAMPLE_DATA_PATH, 'temp_order_ids.txt')
        if os.path.exists(temp_ids_file):
            os.remove(temp_ids_file)
            print("🧹 Archivo temporal de order_ids eliminado")
        
        if missing_files:
            raise Exception(f"Archivos faltantes: {missing_files}")
        
        return "✅ Todos los archivos de muestra creados exitosamente"

    # Orquestación del DAG
    cleanup_task = cleanup_previous_samples()
    orders_sample_result = get_orders_sample()
    
    # Ejecutar en paralelo
    prior_task = load_prior_products(orders_sample_result)
    train_task = load_train_products(orders_sample_result)
    masters_task = load_master_tables()
    
    # Validación final
    validation_task = validate_samples()
    
    # Dependencias
    cleanup_task >> orders_sample_result >> [prior_task, train_task] >> validation_task
    cleanup_task >> masters_task >> validation_task

# Instanciar el DAG
dag = instacart_sample_creation_dag()