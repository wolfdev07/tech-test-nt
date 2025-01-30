import pandas as pd


def csv_uploader(route=None, conn=None): 
    if conn == None or route == None:
        return print("Sin conexión a la base de datos, o ruta de CSV Invalida") 
    
    try:
        df = pd.read_csv(route, low_memory=False)
        df.to_sql("raw_data", conn, if_exists="replace", index=False, chunksize=5000)
        print(f"📊 Total de registros leídos: {df.shape[0]}")

        print("✅ CSV cargado exitosamente en `raw_data`")
        return
    except Exception as e:
        print(f"❌ Error al cargar el CSV: {e}")
        return
    
def csv_xtractor():
    pass