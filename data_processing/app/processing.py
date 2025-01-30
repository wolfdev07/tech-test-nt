import pandas as pd

def csv_uploader(route=None, conn=None): 
    if conn == None or route == None:
        return print("Sin conexión a la base de datos, o ruta de CSV Invalida") 
    
    try:
        df = pd.read_csv(route)
        df.to_sql("raw_data", conn, if_exists="replace", index=False)
    except Exception as e:
        print(e)
        return
    
def csv_xtractor():
    pass