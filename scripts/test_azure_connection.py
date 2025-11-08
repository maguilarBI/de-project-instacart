# test_azure_connection.py
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import logging
import sys
from datetime import datetime
import tempfile
import os

def test_azure_connection(container_name="datalake", wasb_conn_id="utec_blob_storage"):
    """
    Prueba la conexión a Azure Blob Storage y verifica si un contenedor existe.
    Método compatible con versiones antiguas de azure-storage-blob.
    """
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("🔍 Iniciando prueba de conexión a Azure Blob Storage...")
        logger.info(f"📦 Contenedor a verificar: {container_name}")
        logger.info(f"🔗 Usando conexión: {wasb_conn_id}")
        
        # 1. Crear hook
        logger.info("🔄 Creando WasbHook...")
        hook = WasbHook(wasb_conn_id=wasb_conn_id)
        logger.info("✅ WasbHook creado exitosamente")
        
        # 2. Obtener cliente
        logger.info("🔄 Obteniendo cliente de Azure...")
        client = hook.get_conn()
        logger.info("✅ Cliente de Azure obtenido exitosamente")
        
        # 3. Verificación compatible: Listar y buscar
        logger.info(f"🔎 Verificando existencia del contenedor '{container_name}'...")
        
        container_exists = False
        try:
            # Listar todos los contenedores
            containers = client.list_containers()
            container_list = list(containers)
            
            # Buscar por nombre exacto
            for container in container_list:
                if container.name == container_name:
                    container_exists = True
                    logger.info(f"✅ CONTENEDOR ENCONTRADO: '{container_name}'")
                    break
            
            if not container_exists:
                logger.warning(f"❌ CONTENEDOR NO ENCONTRADO: '{container_name}'")
            
            # Mostrar todos los contenedores
            logger.info(f"📂 Todos los contenedores disponibles ({len(container_list)}):")
            for container in container_list:
                marker = " ← ¡ESTE ES!" if container.name == container_name else ""
                logger.info(f"   - '{container.name}'{marker}")
                
        except Exception as list_error:
            logger.error(f"❌ Error al listar contenedores: {list_error}")
            return False
        
        # 4. Probar operaciones si el contenedor existe - MÉTODO COMPATIBLE
        if container_exists:
            try:
                test_blob_name = "airflow_cli_test.txt"
                test_content = f"Test CLI - {datetime.now()}"
                
                # Crear archivo temporal
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as temp_file:
                    temp_file.write(test_content)
                    temp_file_path = temp_file.name
                
                logger.info("🧪 Probando escritura...")
                hook.load_file(
                    file_path=temp_file_path,
                    container_name=container_name,
                    blob_name=test_blob_name,
                    overwrite=True
                )
                logger.info("✅ Escritura exitosa")
                
                logger.info("🧪 Probando lectura...")
                with tempfile.NamedTemporaryFile(mode='r', delete=False, suffix='_download.txt') as download_file:
                    hook.get_file(
                        file_path=download_file.name,
                        container_name=container_name,
                        blob_name=test_blob_name
                    )
                    
                    with open(download_file.name, 'r') as f:
                        downloaded_content = f.read().strip()
                    
                    # Limpiar archivo temporal
                    os.unlink(download_file.name)
                
                if downloaded_content == test_content:
                    logger.info("✅ Lectura exitosa - contenido verificado")
                else:
                    logger.warning("⚠️ Lectura: contenido no coincide")
                
                # Limpiar archivo de test en Azure
                try:
                    container_client = client.get_container_client(container_name)
                    container_client.delete_blob(test_blob_name)
                    logger.info("🧹 Archivo de test eliminado")
                except Exception as delete_error:
                    logger.warning(f"⚠️ No se pudo eliminar archivo de test: {delete_error}")
                
                # Limpiar archivo temporal local
                os.unlink(temp_file_path)
                
            except Exception as test_error:
                logger.warning(f"⚠️ Pruebas fallaron: {test_error}")
                # Limpiar en caso de error
                try:
                    if 'temp_file_path' in locals():
                        os.unlink(temp_file_path)
                except:
                    pass
        
        logger.info("🎉 Prueba de conexión completada")
        return container_exists
        
    except Exception as e:
        logger.error(f"❌ Error general: {str(e)}")
        return False

def main():
    """Función principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Probar conexión a Azure Blob Storage')
    parser.add_argument('--container', '-c', default='datalake',
                       help='Nombre del contenedor a verificar')
    parser.add_argument('--connection', '-conn', default='utec_blob_storage',
                       help='ID de la conexión en Airflow')
    
    args = parser.parse_args()
    
    success = test_azure_connection(
        container_name=args.container,
        wasb_conn_id=args.connection
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()