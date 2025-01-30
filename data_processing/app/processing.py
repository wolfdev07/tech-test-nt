import pandas as pd

def csv_uploader(route=None, conn=None): 
    if conn == None or route == None:
        return print("Sin conexión a la base de datos, o ruta de CSV Invalida") 
    
    try:
        df = pd.read_csv(route, low_memory=False, nrows=1000000)
        print(f"📊 Total de registros leídos: {df.shape[0]}")

        df.to_sql("raw_data", conn, if_exists="replace", index=False, chunksize=5000)

        print("✅ CSV cargado exitosamente en `raw_data`")
    except Exception as e:
        print(f"❌ Error al cargar el CSV: {e}")
        return
    

def db_xtractor(db_name="raw_data", conn=None):
    if conn == None:
        return print("Sin conexión a la base de datos.")
    
    try:
        df = pd.read_sql(f"SELECT * FROM {db_name}", conn)
        df.to_parquet("output.parquet")
        print("✅ Datos extraídos en formato Parquet")
    except Exception as e:
        print(e)
        return