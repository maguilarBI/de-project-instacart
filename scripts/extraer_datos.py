import pandas as pd
import os

def extraer_datos(archivo_salida):
    # Ruta del archivo de entrada
    archivo_entrada = '/home/maguilar/airflow-project/data/datos_entrada.csv'
    
    # Verificar que existe el archivo de entrada
    if not os.path.exists(archivo_entrada):
        raise FileNotFoundError(f"❌ No se encuentra: {archivo_entrada}")
    
    # Leer datos
    datos = pd.read_csv(archivo_entrada)
    print(f"📥 Datos extraídos: {len(datos)} registros")
    
    # Guardar datos extraídos
    datos.to_csv(archivo_salida, index=False)
    print(f"💾 Datos extraídos guardados en: {archivo_salida}")
    
    return f"Extracción completada: {len(datos)} registros"