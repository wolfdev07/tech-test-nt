import pandas as pd
from sqlalchemy.exc import IntegrityError

def csv_uploader(route=None, conn=None):
    if route == None:
        print("Ruta de CSV Invalida") 
        return
    
    try:
        df = pd.read_csv(route, low_memory=False, nrows=1000000)
        print(f"📊 Total de registros leídos: {df.shape[0]}")
        with conn.connect() as connection:
            df.to_sql("raw_data", connection, if_exists="replace", index=False, chunksize=5000)
        #df.to_sql("raw_data", conn, if_exists="replace", index=False, chunksize=5000)
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
        with conn.connect() as connection:
            df = pd.read_sql(f"SELECT * FROM {db_name}", connection)
        #df = pd.read_sql(f"SELECT * FROM {db_name}", conn)
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
        with conn.connect() as connection:
            df_transformed.to_sql("cargo", connection, if_exists="replace", index=False, chunksize=5000)

        #df_transformed.to_sql("cargo", conn, if_exists="replace", index=False, chunksize=5000)        
        print("✅ CSV cargado exitosamente en `cargo`")
        return
    except Exception as e:
        print(f"❌ Error al transformar los datos: {e}")
        return None


def disperse_data(route=None, conn=None):
    if conn is None or route is None:
        print("\u274C Conexión inválida o archivo no encontrado.")
        return
    
    try:
        df = pd.read_csv(route, low_memory=False)
        print("\U0001F4CA Dispersando información...")

        with conn.connect() as connection:
            # Insertar en `companies` evitando duplicados
            if "company_id" in df.columns and "name" in df.columns:
                df_companies = df[["company_id", "name"]].drop_duplicates()
                df_companies = df_companies.dropna(subset=["company_id", "name"])
                df_companies = df_companies.rename(columns={"company_id": "id"})
                
                existing_companies = pd.read_sql("SELECT id FROM companies", connection)
                df_companies = df_companies[~df_companies["id"].isin(existing_companies["id"])]
                
                if not df_companies.empty:
                    df_companies.to_sql("companies", connection, if_exists="append", index=False)

            # Insertar en `charges`
            required_columns = {"id", "company_id", "amount", "status", "created_at", "updated_at"}
            if required_columns.issubset(df.columns):
                df_charges = df[list(required_columns)].dropna(subset=["id", "company_id", "amount", "status", "created_at"])
                
                # Convertir fechas a formato timestamp
                df_charges["created_at"] = pd.to_datetime(df_charges["created_at"], errors="coerce")
                df_charges["updated_at"] = pd.to_datetime(df_charges["updated_at"], errors="coerce")
                
                # Verificar que `company_id` en charges exista en companies
                valid_company_ids = pd.read_sql("SELECT id FROM companies", connection)
                df_charges = df_charges[df_charges["company_id"].isin(valid_company_ids["id"])]
                
                if not df_charges.empty:
                    df_charges.to_sql("charges", connection, if_exists="append", index=False)

        print("\u2705 Dispersión de datos completada.")
    except IntegrityError as e:
        print(f"\u274C Error de integridad en la base de datos: {e}")
    except Exception as e:
        print(f"\u274C Error al dispersar datos: {e}")

