"""imprimirá el menú de consultas planeadas para el proyecto (sin lógica
funcional aún)."""

from connect import init_connections, close_connections


"""
USO DE IA
Chat me ayudo a crear el menu con las queries que se plantearon
en el menu es la misma sinaxis que en los laboratiorios a excepcion de 
las connecciones que se usan con el diccionario
"""


def print_menu():
    print("\n" + "="*20)
    print("App de Habitos y Bienestar")
    print("="*20)
    cassandra_options = {
        1: "Historial de actividad reciente del usuario",
        2: "Actividad en un día específico",
        3: "Cumplimiento de hábito por usuario",
        4: "Próximos recordatorios del día",
        5: "Registros de estado de ánimo",
        6: "Mostrar rutina por categoría",
        7: "Retos activos en participación por usuario",
        8: "Top de participantes por reto",
    }

    mongo_options = {
        9:  "Hábitos activos de un usuario",
        10: "Usuarios con más de 5 hábitos",
        11: "Promedio de cumplimiento",
        12: "Hábitos más frecuentes",
        13: "Usuarios con bajo rendimiento",
        14: "Hábitos por categoría",
        15: "Usuarios con registros en un día específico",
        16: "Distribución de estados",
        17: "Logros e insignias por usuario",
        18: "Hábitos con mayor tasa de abandono",
    }

    dgraph_options = {
        19: "Recomendación de hábitos",
        20: "Amigos con intereses similares",
        21: "Usuarios influyentes",
        22: "Hábitos más conectados",
        23: "Hábitos compartidos entre amigos",
        24: "Retos recomendados por afinidad social",
        25: "Actividades recomendadas por categorías",
        26: "Eventos recomendados por categoría y amigos",
    }
    print("\n--- CASSANDRA ---")
    for key, value in cassandra_options.items():
        print(key, "--", value)

    print("\n--- MONGODB ---")
    for key, value in mongo_options.items():
        print(key, "--", value)

    print("\n--- DGRAPH ---")
    for key, value in dgraph_options.items():
        print(key, "--", value)

    print("\n0 -- Salir")
    print("="*50)


def main():
    print("Iniciando conexiones...")
    conexion = init_connections()
    print("¡Conexiones establecidas!")

    cassandra_session = conexion["cassandra_session"]
    mongo_db          = conexion["mongo_url"]
    dgraph_client     = conexion["dgraph_client"]

    while True:
        print_menu()
        try:
            option = int(input("\nSelecciona una opción: "))
        except ValueError:
            print("Por favor ingresa un número válido.")
            continue

        # ── CASSANDRA ──────────────────────────────────────────
        if option == 1:
            print("\n[Cassandra] Historial de actividad reciente del usuario")
            # model_cassandra.req1(cassandra_session)
        elif option == 2:
            print("\n[Cassandra] Actividad en un día específico")
            # model_cassandra.req2(cassandra_session)
        elif option == 3:
            print("\n[Cassandra] Cumplimiento de hábito por usuario")
            # model_cassandra.req3(cassandra_session)
        elif option == 4:
            print("\n[Cassandra] Próximos recordatorios del día")
            # model_cassandra.req4(cassandra_session)
        elif option == 5:
            print("\n[Cassandra] Registros de estado de ánimo")
            # model_cassandra.req5(cassandra_session)
        elif option == 6:
            print("\n[Cassandra] Mostrar rutina por categoría")
            # model_cassandra.req6(cassandra_session)
        elif option == 7:
            print("\n[Cassandra] Retos activos en participación por usuario")
            # model_cassandra.req7(cassandra_session)
        elif option == 8:
            print("\n[Cassandra] Top de participantes por reto")
            # model_cassandra.req8(cassandra_session)

        # ── MONGODB ────────────────────────────────────────────
        elif option == 9:
            print("\n[MongoDB] Hábitos activos de un usuario")
            # model_mongo.req1(mongo_db)
        elif option == 10:
            print("\n[MongoDB] Usuarios con más de 5 hábitos")
            # model_mongo.req2(mongo_db)
        elif option == 11:
            print("\n[MongoDB] Promedio de cumplimiento")
            # model_mongo.req3(mongo_db)
        elif option == 12:
            print("\n[MongoDB] Hábitos más frecuentes")
            # model_mongo.req4(mongo_db)
        elif option == 13:
            print("\n[MongoDB] Usuarios con bajo rendimiento")
            # model_mongo.req5(mongo_db)
        elif option == 14:
            print("\n[MongoDB] Hábitos por categoría")
            # model_mongo.req6(mongo_db)
        elif option == 15:
            print("\n[MongoDB] Usuarios con registros en un día específico")
            # model_mongo.req7(mongo_db)
        elif option == 16:
            print("\n[MongoDB] Distribución de estados")
            # model_mongo.req8(mongo_db)
        elif option == 17:
            print("\n[MongoDB] Logros e insignias por usuario")
            # model_mongo.req9(mongo_db)
        elif option == 18:
            print("\n[MongoDB] Hábitos con mayor tasa de abandono")
            # model_mongo.req10(mongo_db)

        # ── DGRAPH ─────────────────────────────────────────────
        elif option == 19:
            print("\n[Dgraph] Recomendación de hábitos")
            # model_dgraph.req1(dgraph_client)
        elif option == 20:
            print("\n[Dgraph] Amigos con intereses similares")
            # model_dgraph.req2(dgraph_client)
        elif option == 21:
            print("\n[Dgraph] Usuarios influyentes")
            # model_dgraph.req3(dgraph_client)
        elif option == 22:
            print("\n[Dgraph] Hábitos más conectados")
            # model_dgraph.req4(dgraph_client)
        elif option == 23:
            print("\n[Dgraph] Hábitos compartidos entre amigos")
            # model_dgraph.req5(dgraph_client)
        elif option == 24:
            print("\n[Dgraph] Retos recomendados por afinidad social")
            # model_dgraph.req6(dgraph_client)
        elif option == 25:
            print("\n[Dgraph] Actividades recomendadas por categorías")
            # model_dgraph.req7(dgraph_client)
        elif option == 26:
            print("\n[Dgraph] Eventos recomendados por categoría y amigos")
            # model_dgraph.req8(dgraph_client)

        # ── SISTEMA ────────────────────────────────────────────
        elif option == 0:
            print("\nCerrando conexiones...")
            close_connections(conexion)
            print("Adios master")
            break

        else:
            print("Opción no válida, intenta de nuevo.")



if __name__ == '__main__':
    main()