from sqlalchemy import create_engine

def get_connection(pg_url=None):
    if pg_url == None:
        return print("Url de conexión invalida.")
    try:
        engine = create_engine(url=pg_url)
        return engine
    except Exception as e:
        return print(e)
