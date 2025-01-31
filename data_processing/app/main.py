import os
import colorama
from decouple import config
from processing import csv_uploader, db_xtractor, data_transformation, disperse_data
from database import get_connection
from models import init_db

def data_processing():
    csv_url = os.path.join(os.getcwd(), "data_prueba_tecnica.csv")
    cargo_csv = os.path.join(os.getcwd(), "cargo.csv")
    engine = get_connection(pg_url=config("PG_URL", default=None))

    if engine is None:
        print("❌ No se pudo conectar a la base de datos.")
        return

    # Inicializa la base de datos solo una vez
    init_db(engine)

    print(colorama.Fore.CYAN + "=" * 40)
    print(colorama.Fore.GREEN + "   *** PROCESADOR DE CSV ***")
    print(colorama.Fore.CYAN + "=" * 40 + "\n")

    while True:
        print(colorama.Fore.YELLOW + "Elige la opción deseada:")
        print("  (1) Cargar CSV a la base de datos")
        print("  (2) Extraer Base de datos a CSV")
        print("  (3) Transformar Información")
        print("  (4) Dispersar Información")
        print("  (5) Salir\n")

        user_selection = input(">> ")

        if user_selection == "1":
            if os.path.exists(csv_url):
                csv_uploader(conn=engine, route=csv_url)
            else:
                print("❌ El archivo CSV no existe.")

        elif user_selection == "2":
            db_xtractor(conn=engine)

        elif user_selection == "3":
            if os.path.exists(cargo_csv):
                data_transformation(route=cargo_csv, conn=engine)
                print(colorama.Fore.GREEN + "Transformación realizada correctamente.\n")
            else:
                print("❌ El archivo de transformación no existe.")

        elif user_selection == "4":
            disperse_data(route=cargo_csv, conn=engine)

        elif user_selection == "5":
            print(colorama.Fore.RED + "Saliendo del programa...\n")
            return

if __name__ == "__main__":
    data_processing()
