import pandas as pd
import os

def cargar_datos(archivo_entrada, archivo_salida):
    # Verificar que existe el archivo de entrada
    if not os.path.exists(archivo_entrada):
        raise FileNotFoundError(f"❌ No se encuentra: {archivo_entrada}")
    
    # Leer datos transformados
    datos_transformados = pd.read_csv(archivo_entrada)
    print(f"💾 Cargando {len(datos_transformados)} registros a salida final")
    
    # Guardar datos finales
    datos_transformados.to_csv(archivo_salida, index=False)
    
    print(f"✅ Datos cargados en: {archivo_salida}")
    print("📊 Resumen final:")
    print(datos_transformados)
    
    return f"Carga completada: {len(datos_transformados)} registros en {archivo_salida}"