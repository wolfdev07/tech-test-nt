import pandas as pd
from sqlalchemy.orm import sessionmaker
from models import Company, Charge

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
            df.to_csv("output.csv")
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
            print("❌ El DataFrame de output está vacío, no hay datos para transformar.")
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
    if conn is None:
        print("❌ Sin conexión a la base de datos.")
        return

    try:
        # Cargar el CSV en un DataFrame
        data_frame = pd.read_csv(route, low_memory=False, nrows=1000000)

        if data_frame.empty:
            print("❌ No hay datos para dispersar.")
            return

        print("⏳ Dispersando datos...")

        # Crear sesión SQLAlchemy
        Session = sessionmaker(bind=conn)
        session = Session()

        # Extraer compañías únicas
        companies_df = data_frame[['company_id', 'name']].drop_duplicates().dropna(subset=['company_id', 'name'])

        # Insertar compañías evitando duplicados
        for _, row in companies_df.iterrows():
            existing_company = session.query(Company).filter_by(id=row['company_id']).first()
            if not existing_company:
                new_company = Company(id=row['company_id'], name=row['name'])
                session.add(new_company)

        session.commit()  # Guardar cambios en la BD

        # Extraer transacciones
        charges_df = data_frame[['id', 'company_id', 'amount', 'status', 'created_at', 'paid_at']].dropna(subset=['id', 'company_id'])

        # Convertir fechas
        for col in ["created_at", "paid_at"]:
            if col in data_frame.columns:
                data_frame[col] = pd.to_datetime(data_frame[col], format='%Y-%m-%d', errors='coerce')

        # Reemplazar NaT por None para evitar errores en PostgreSQL
        charges_df['paid_at'] = charges_df['paid_at'].apply(lambda x: None if pd.isna(x) else x)

        # Limitar amount para evitar overflow en PostgreSQL
        max_value = 10**14 - 1  # Máximo permitido por DECIMAL(16,2)
        charges_df['amount'] = charges_df['amount'].apply(lambda x: min(x, max_value) if pd.notna(x) else 0)

        # Insertar transacciones respetando la relación con Company
        for _, row in charges_df.iterrows():
            new_charge = Charge(
                id=row['id'],
                company_id=row['company_id'],
                amount=row['amount'],
                status=row['status'],
                created_at=row['created_at'],
                paid_at=row['paid_at']
            )
            session.add(new_charge)

        session.commit()  # Guardar cambios en la BD
        session.close()

        print("✅ Datos dispersados correctamente en `companies` y `charges`.")

    except Exception as e:
        session.rollback()  # Deshacer cambios en caso de error
        print(f"❌ Error al dispersar los datos: {e}")
