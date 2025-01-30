import os
import colorama
from extraction import csv_xtractor
from database import conn

def app_xtract():
    csv_url = os.path.join(os.getcwd(), "data.csv")

    print(colorama.Fore.CYAN + "=" * 40)
    print(colorama.Fore.GREEN + "   *** PROCESADOR DE CSV ***")
    print(colorama.Fore.CYAN + "=" * 40 + "\n")

    while True:
        print(colorama.Fore.YELLOW + "Elige la opción deseada:")
        print("  (1) Extraer CSV")
        print("  (2) Cambiar ubicación de CSV")
        print("  (3) Salir\n")

        user_selection = input(">> ")
        
        if user_selection == "1":
            break
        elif user_selection == "2":
            csv_url = input(colorama.Fore.BLUE + "Ingrese la nueva ruta del CSV: ")
            print(colorama.Fore.GREEN + "Ruta actualizada correctamente.\n")
        elif user_selection == "3":
            print(colorama.Fore.RED + "Saliendo del programa...\n")
            return
    
    print(colorama.Fore.CYAN + "\nIniciando extracción...\n")
    csv_xtractor(conn=conn, route=csv_url)


if __name__ == "__main__":
    app_xtract()
