from datetime import datetime
import pandas as pd

def csv_uploader(route=None, conn=None):
    if conn == None or route == None:
        return print("Sin conexión a la base de datos, o ruta de CSV Invalida") 
    
    try:
        df = pd.read_csv(route, low_memory=False, nrows=1000000)
        print(f"📊 Total de registros leídos: {df.shape[0]}")
        transformation = data_transformation(data_frame=df, conn=conn)
        if transformation is not None:
            return print("✅ CSV cargado exitosamente en `cargo`")
        return print(f"❌ Error al cargar el CSV.")
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


def data_transformation(data_frame=None, conn=None):
    if data_frame is None or conn is None:
        return print("❌ Sin conexión a la base de datos.")

    if data_frame.empty:
        return print("❌ El DataFrame está vacío, no hay datos para transformar.")
    
    try:
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
            df_transformed[col] = pd.to_datetime(df_transformed[col], format='%Y-%m-%d', errors='coerce')

        df_transformed = df_transformed.where(pd.notnull(df_transformed), None)
        df_transformed.to_sql("cargo", conn, if_exists="replace", index=False, chunksize=5000)
        
        return df_transformed
    except Exception as e:
        print(f"❌ Error al transformar los datos: {e}")
        return None