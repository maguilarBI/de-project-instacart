from datetime import datetime
import os
import pandas as pd

def add_date_suffix(filename, date=None):
    """
    Appends _YYYYMMDD to the filename before the extension.
    
    Args:
        filename (str): Original filename (e.g. 'data.csv')
        date (datetime, optional): Date to use; defaults to today.
    
    Returns:
        str: Modified filename (e.g. 'data_20250501.csv')
    """
    if date is None:
        date = datetime.today()
        
    name, ext = os.path.splitext(filename)
    date_str = date.strftime("%Y%m%d")
    return f"{name}_{date_str}{ext}"

def sample_by_users(orders_df, sample_frac=0.1, random_state=42):
    """
    ✅ NUEVA FUNCIÓN: Muestreo por usuarios para mantener integridad referencial
    """
    # Obtener usuarios únicos
    unique_users = orders_df['user_id'].unique()
    
    # Tomar muestra de usuarios
    sample_users = pd.Series(unique_users).sample(
        frac=sample_frac, 
        random_state=random_state
    )
    
    # Filtrar órdenes de los usuarios muestreados
    orders_sample = orders_df[orders_df['user_id'].isin(sample_users)]
    
    print(f"📊 Muestreo: {len(sample_users)} usuarios → {len(orders_sample)} órdenes")
    return orders_sample

def validate_referential_integrity(parent_df, child_df, key='order_id'):
    """
    ✅ NUEVA FUNCIÓN: Valida integridad referencial entre tablas
    """
    parent_ids = set(parent_df[key])
    child_ids = set(child_df[key])
    
    missing_ids = child_ids - parent_ids
    
    if missing_ids:
        print(f"⚠️  Advertencia: {len(missing_ids)} {key}s no encontrados en tabla padre")
        print(f"   Ejemplos: {list(missing_ids)[:5]}")
        return False
    else:
        print(f"✅ Integridad referencial validada: todos los {key}s existen")
        return True