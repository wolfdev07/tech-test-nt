import pandas as pd

def csv_uploader(route=None, conn=None):
    if route == None:
        print("Ruta de CSV Invalida") 
        return
    
    try:
        df = pd.read_csv(route, low_memory=False, nrows=1000000)
        print(f"📊 Total de registros leídos: {df.shape[0]}")
        df.to_sql("raw_data", conn, if_exists="replace", index=False, chunksize=5000)
        print("✅ CSV cargado exitosamente en `raw_data`")
        return
    except Exception as e:
        print(f"❌ Error al cargar el CSV: {e}")
        return


def db_xtractor(db_name="raw_data", conn=None):
    if conn == None:
        print("Sin conexión a la base de datos.")
        return
    
    try:
        df = pd.read_sql(f"SELECT * FROM {db_name}", conn)
        df.to_csv("cargo.csv")
        print("✅ Datos extraídos en formato CSV \n")
    except Exception as e:
        print(e)
        return


def data_transformation(route=None, conn=None):
    if conn is None:
        print("❌ Sin conexión a la base de datos.")
        return
    
    try:
        data_frame = pd.read_csv(route, low_memory=False, nrows=1000000)
        if data_frame.empty:
            print("❌ El DataFrame de cargo está vacío, no hay datos para transformar.")
            return
        print(f"⏳ Transformando datos...")
        df_transformed = data_frame.rename(columns={
            "id": "id",
            "company_name": "company_name",
            "company_id": "company_id",
            "amount": "amount",
            "status": "status",
            "created_at": "created_at",
            "updated_at": "updated_at"
        })
        for col in ["created_at", "paid_at"]:
            if col in df_transformed.columns:
                df_transformed[col] = pd.to_datetime(df_transformed[col], format='%Y-%m-%d', errors='coerce')

        df_transformed = df_transformed.where(pd.notnull(df_transformed), None)
        df_transformed.to_sql("cargo", conn, if_exists="replace", index=False, chunksize=5000)
        
        print("✅ CSV cargado exitosamente en `cargo`")
        return
    except Exception as e:
        print(f"❌ Error al transformar los datos: {e}")
        return None


def disperse_data(route=None, conn=None):
    pass