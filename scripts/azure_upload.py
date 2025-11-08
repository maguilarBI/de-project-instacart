from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import logging
import os

log = logging.getLogger('airflow.task')

def upload_to_adls(
        local_file_path="/home/maguilar/airflow-project/data/sample.txt",
        container_name="datalake", 
        blob_name="raw/airflow/G2/archivo_subido.txt",
        wasb_conn_id="utec_blob_storage"  
        ):
    """
    Sube archivo local a Azure Blob Storage usando conexión de Airflow
    """
    try:
        if not os.path.exists(local_file_path):
            log.warning(f"==> Local file not found: {local_file_path}")
            return False
        
        file_size = os.path.getsize(local_file_path) / (1024 * 1024)  # MB
        log.info(f"==> Uploading {local_file_path} ({file_size:.2f} MB) to {container_name}/{blob_name}")
        
        # Conexión a Azure usando la configuración de Airflow
        hook = WasbHook(wasb_conn_id=wasb_conn_id)    
        hook.load_file(
            file_path=local_file_path,
            container_name=container_name,
            blob_name=blob_name,
            overwrite=True
        )

        log.info(f"==> Successfully uploaded {local_file_path} to {container_name}/{blob_name}")
        return True
        
    except Exception as e:
        log.error(f"==> Failed to upload {local_file_path} to Azure Blob: {str(e)}", exc_info=True)
        raise