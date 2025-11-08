import pandas as pd
import os

def transformar_datos(archivo_entrada, archivo_salida):
    # Verificar que existe el archivo de entrada
    if not os.path.exists(archivo_entrada):
        raise FileNotFoundError(f"❌ No se encuentra: {archivo_entrada}")
    
    # Leer datos extraídos
    datos = pd.read_csv(archivo_entrada)
    print(f"🔄 Transformando {len(datos)} registros")
    
    # Verificar columnas necesarias
    if 'fecha' not in datos.columns or 'ventas' not in datos.columns:
        raise ValueError("❌ El archivo no tiene las columnas 'fecha' y 'ventas'")
    
    # Transformar: Agrupar por fecha y sumar ventas
    ventas_por_dia = datos.groupby('fecha')['ventas'].sum().reset_index()
    ventas_por_dia['ventas_totales'] = ventas_por_dia['ventas']
    
    # Guardar datos transformados
    resultado = ventas_por_dia[['fecha', 'ventas_totales']]
    resultado.to_csv(archivo_salida, index=False)
    
    print(f"✅ Datos transformados guardados en: {archivo_salida}")
    print(resultado.head())
    
    return f"Transformación completada: {len(resultado)} días"