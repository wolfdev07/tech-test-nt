import os
import colorama
from processing import csv_uploader, csv_xtractor
from database import conn

def data_processing():
    csv_url = os.path.join(os.getcwd(), "data.csv")

    print(colorama.Fore.CYAN + "=" * 40)
    print(colorama.Fore.GREEN + "   *** PROCESADOR DE CSV ***")
    print(colorama.Fore.CYAN + "=" * 40 + "\n")

    while True:
        print(colorama.Fore.YELLOW + "Elige la opción deseada:")
        print("  (1) Cargar CSV a la base de datos")
        print("  (2) Extraer Base de datos a CSV")
        print("  (3) Cambiar ubicación de CSV")
        print("  (4) Salir\n")

        user_selection = input(">> ")
        
        if user_selection == "1":
            """
                CHARGE DATABASE FROM CSV
            """
            print(colorama.Fore.CYAN + "\nIniciando carga...\n")
            csv_uploader(conn=conn, route=csv_url)
            return
        elif user_selection == "2":
            """
                EXTRACT DATABASE TO CSV
            """
            print(colorama.Fore.CYAN + "\nIniciando extracción...\n")
            csv_xtractor()
            return
        elif user_selection == "3":
            """
                CHANGE CSV LOCATION
            """
            csv_url = input(colorama.Fore.BLUE + "Ingrese la nueva ruta del CSV: ")
            print(colorama.Fore.GREEN + "Ruta actualizada correctamente.\n")
        elif user_selection == "4":
            """
                EXIT PROGRAM
            """
            print(colorama.Fore.RED + "Saliendo del programa...\n")
            return


if __name__ == "__main__":
    data_processing()
