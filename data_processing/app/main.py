import os
import colorama
from decouple import config
from processing import csv_uploader, db_xtractor, data_transformation, disperse_data
from database import get_connection
from models import init_db

def data_processing():
    csv_url = os.path.join(os.getcwd(), "data_prueba_tecnica.csv")
    cargo_csv = os.path.join(os.getcwd(), "cargo.csv")
    engine = get_connection(pg_url=config("PG_URL") if config("PG_URL") else None)

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
            """
                CHARGE DATABASE FROM CSV
            """
            print(colorama.Fore.CYAN + "\nIniciando carga...\n")
            csv_uploader(conn=engine, route=csv_url)
        elif user_selection == "2":
            """
                EXTRACT DATABASE TO CSV
            """
            print(colorama.Fore.CYAN + "\nIniciando extracción...\n")
            db_xtractor(conn=engine)
        elif user_selection == "3":
            """
                DATA TRANSFORMATION
            """
            data_transformation(route=cargo_csv, conn=engine)
            print(colorama.Fore.GREEN + "Transformación realizada correctamente.\n")
        elif user_selection == "4":
            """
                DISPERCE DATA
            """
            init_db(engine=engine)
            disperse_data(route=cargo_csv, conn=engine)
            print(colorama.Fore.GREEN + "Disperción realizada correctamente.\n")
        elif user_selection == "5":
            """
                EXIT PROGRAM
            """
            print(colorama.Fore.RED + "Saliendo del programa...\n")
            return


if __name__ == "__main__":
    data_processing()
