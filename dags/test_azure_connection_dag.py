from airflow.decorators import dag, task
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from pendulum import timezone
from datetime import datetime
import logging
import tempfile
import os

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'retries': 0,
}

@dag(
    dag_id='test_azure_connection_dag',
    default_args=default_args,
    description='DAG para probar la conexión a Azure Blob Storage',
    start_date=datetime(2025, 1, 1, tzinfo=timezone("America/Bogota")),
    schedule="0 12 * * 1", # Runs every monday at 12:00 local time (GMT-5)
    catchup=False,
    tags=['test', 'azure', 'connection'],
)
def test_azure_connection_dag():

    @task
    def test_azure_connection():
        """
        Prueba la conexión a Azure Blob Storage y verifica si el contenedor existe
        """
        container_name = "datalake"
        wasb_conn_id = "utec_blob_storage"
        
        try:
            logger.info("🔍 Iniciando prueba de conexión a Azure Blob Storage...")
            logger.info(f"📦 Contenedor a verificar: {container_name}")
            logger.info(f"🔗 Usando conexión: {wasb_conn_id}")
            
            # 1. Crear el hook
            logger.info("🔄 Creando WasbHook...")
            hook = WasbHook(wasb_conn_id=wasb_conn_id)
            logger.info("✅ WasbHook creado exitosamente")
            
            # 2. Obtener cliente de Azure
            logger.info("🔄 Obteniendo cliente de Azure...")
            client = hook.get_conn()
            logger.info("✅ Cliente de Azure obtenido exitosamente")
            
            # 3. Verificar existencia del contenedor
            logger.info(f"🔎 Verificando existencia del contenedor '{container_name}'...")
            
            container_exists = False
            actual_container_name = None
            
            try:
                # Método universal: listar todos los contenedores y buscar por nombre
                containers = client.list_containers()
                container_list = list(containers)
                
                # Buscar el contenedor por nombre (case-sensitive)
                for container in container_list:
                    if container.name == container_name:
                        container_exists = True
                        actual_container_name = container.name
                        break
                
                if container_exists:
                    logger.info(f"✅ El contenedor '{container_name}' EXISTE en Azure Blob Storage")
                    
                    # Intentar listar blobs con método compatible
                    try:
                        container_client = client.get_container_client(container_name)
                        blobs = container_client.list_blobs()
                        blob_list = list(blobs)
                        
                        logger.info(f"📁 Contenedor '{container_name}' contiene {len(blob_list)} blobs:")
                        for i, blob in enumerate(blob_list[:5]):
                            logger.info(f"   {i+1}. {blob.name} ({blob.size} bytes)")
                        
                        if len(blob_list) > 5:
                            logger.info(f"   ... y {len(blob_list) - 5} más")
                            
                    except Exception as list_error:
                        logger.warning(f"⚠️ No se pudieron listar blobs: {list_error}")
                        
                else:
                    logger.warning(f"❌ El contenedor '{container_name}' NO EXISTE en Azure Blob Storage")
                    
                # Mostrar todos los contenedores disponibles
                logger.info(f"📂 Contenedores disponibles ({len(container_list)}):")
                for container in container_list:
                    status = " ← TARGET" if container.name == container_name else ""
                    logger.info(f"   - {container.name}{status}")
                    
            except Exception as container_error:
                logger.error(f"❌ Error al listar contenedores: {container_error}")
                return {
                    "status": "error",
                    "container_exists": False,
                    "message": f"Error al listar contenedores: {str(container_error)}"
                }
            
            # 4. SOLO SI EL CONTENEDOR EXISTE: Probar operaciones de lectura/escritura
            if container_exists:
                test_result = "No probado"
                try:
                    test_blob_name = "airflow_connection_test.txt"
                    test_content = f"Test de conexión Airflow - {datetime.now()}"
                    
                    # Crear archivo temporal para la prueba
                    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as temp_file:
                        temp_file.write(test_content)
                        temp_file_path = temp_file.name
                    
                    logger.info("🧪 Probando operación de escritura...")
                    
                    # MÉTODO COMPATIBLE: usar load_file en lugar de load_bytes
                    hook.load_file(
                        file_path=temp_file_path,
                        container_name=container_name,
                        blob_name=test_blob_name,
                        overwrite=True
                    )
                    logger.info("✅ Escritura de test exitosa")
                    
                    logger.info("🧪 Probando operación de lectura...")
                    
                    # MÉTODO COMPATIBLE: usar get_file en lugar de read_bytes
                    with tempfile.NamedTemporaryFile(mode='r', delete=False, suffix='_download.txt') as download_file:
                        hook.get_file(
                            file_path=download_file.name,
                            container_name=container_name,
                            blob_name=test_blob_name
                        )
                        
                        # Leer el contenido descargado
                        with open(download_file.name, 'r') as f:
                            downloaded_content = f.read().strip()
                        
                        # Limpiar archivo temporal de descarga
                        os.unlink(download_file.name)
                    
                    if downloaded_content == test_content:
                        logger.info("✅ Lectura de test exitosa - Contenido verificado")
                        test_result = "Éxito"
                    else:
                        logger.warning(f"⚠️ Lectura exitosa pero contenido no coincide")
                        logger.warning(f"   Esperado: {test_content}")
                        logger.warning(f"   Obtenido: {downloaded_content}")
                        test_result = "Contenido no coincide"
                    
                    # Limpiar archivo de test en Azure - MÉTODO COMPATIBLE
                    try:
                        # Intentar con el método del hook si existe
                        if hasattr(hook, 'delete_file'):
                            hook.delete_file(
                                container_name=container_name,
                                blob_name=test_blob_name
                            )
                        else:
                            # Usar el cliente directo
                            container_client = client.get_container_client(container_name)
                            container_client.delete_blob(test_blob_name)
                        logger.info("🧹 Archivo de test eliminado de Azure")
                    except Exception as delete_error:
                        logger.warning(f"⚠️ No se pudo eliminar el archivo de test: {delete_error}")
                    
                    # Limpiar archivo temporal local
                    os.unlink(temp_file_path)
                    
                except Exception as test_ops_error:
                    logger.error(f"❌ Operaciones de test fallaron: {test_ops_error}")
                    test_result = f"Error: {str(test_ops_error)}"
                    
                    # Asegurar limpieza de archivos temporales en caso de error
                    try:
                        if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
                            os.unlink(temp_file_path)
                        if 'download_file' in locals() and 'download_file' in locals() and os.path.exists(download_file.name):
                            os.unlink(download_file.name)
                    except:
                        pass
            else:
                test_result = "No aplica - contenedor no existe"
            
            # Resultado final
            if container_exists:
                message = f"✅ Conexión exitosa. Contenedor '{container_name}' EXISTE. Pruebas: {test_result}"
                logger.info("🎉 Prueba de conexión completada exitosamente")
            else:
                message = f"⚠️ Conexión exitosa pero contenedor '{container_name}' NO EXISTE"
                logger.warning("⚠️ Conexión funcionando pero contenedor objetivo no encontrado")
            
            return {
                "status": "success",
                "container_exists": container_exists,
                "actual_container_name": actual_container_name,
                "test_operations": test_result,
                "message": message
            }
            
        except Exception as e:
            error_msg = f"❌ Error en la prueba de conexión: {str(e)}"
            logger.error(error_msg)
            logger.error("💡 Posibles causas:")
            logger.error("   - La conexión 'utec_blob_storage' no está configurada en Airflow")
            logger.error("   - Las credenciales de Azure son incorrectas o han expirado")
            logger.error("   - Problemas de red/firewall")
            logger.error("   - El nombre del contenedor es incorrecto")
            
            return {
                "status": "error",
                "container_exists": False,
                "message": error_msg
            }

    test_task = test_azure_connection()

dag = test_azure_connection_dag()