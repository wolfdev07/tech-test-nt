from sqlalchemy import create_engine

def get_connection(pg_url=None):
    if not pg_url:
        print("❌ URL de conexión inválida.")
        return None
    try:
        engine = create_engine(pg_url)
        return engine
    except Exception as e:
        print(f"❌ Error en la conexión a la base de datos: {e}")
        return None
