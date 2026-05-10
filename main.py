"""Imprime un menú de consultas del proyecto organizado por funcionalidad,
no por base de datos.
"""

from connect import init_connections, close_connections
from Dgraph import model as model_dgraph
from Cassandra import model as model_cassandra
from Mongo import model as model_mongo

"""
USO DE IA
Chat me ayudó a reorganizar el menú para que no muestre todas las opciones
de golpe ni esté separado por base de datos, sino por funcionalidades del
sistema como hábitos, eventos, perfil y retos.
"""

def print_main_menu():
    print("\n" + "=" * 45)
    print("     App de Hábitos y Bienestar")
    print("=" * 45)
    print("1 -- Hábitos")
    print("2 -- Eventos y actividad")
    print("3 -- Bienestar y perfil del usuario")
    print("4 -- Rutinas y recordatorios")
    print("5 -- Retos")
    print("6 -- Social y recomendaciones")
    print("0 -- Salir")
    print("=" * 45)


def print_submenu(title, options):
    print(f"\n--- {title.upper()} ---")
    for key, value in options.items():
        print(f"{key} -- {value}")
    print("0 -- Volver al menú principal")


# =========================
# FUNCIONES DE SUBMENÚ
# =========================

def habits_menu(cassandra_session, mongo_db, dgraph_client):
    options = {
        1: "Hábitos activos de un usuario",
        2: "Cumplimiento de hábito por usuario",
        3: "Promedio de cumplimiento",
        4: "Hábitos más frecuentes",
        5: "Hábitos por categoría",
        6: "Hábitos con mayor tasa de abandono",
        7: "Recomendación de hábitos",
        8: "Hábitos más conectados",
        9: "Hábitos compartidos entre amigos",
    }

    while True:
        print_submenu("Hábitos", options)
        try:
            option = int(input("\nSelecciona una opción: "))
        except ValueError:
            print("Por favor ingresa un número válido.")
            continue

        if option == 1:
            print("\nHábitos activos de un usuario")
            model_mongo.req1(mongo_db)
        elif option == 2:
            print("\nCumplimiento de hábito por usuario")
            model_cassandra.req3(cassandra_session)
        elif option == 3:
            print("\nPromedio de cumplimiento")
            model_mongo.req3(mongo_db)
        elif option == 4:
            print("\nHábitos más frecuentes")
            model_mongo.req4(mongo_db)
        elif option == 5:
            print("\nHábitos por categoría")
            model_mongo.req6(mongo_db)
        elif option == 6:
            print("\nHábitos con mayor tasa de abandono")
            model_mongo.req10(mongo_db)
        elif option == 7:
            print("\nRecomendación de hábitos")
            model_dgraph.req1(dgraph_client)
        elif option == 8:
            print("\nHábitos más conectados")
            model_dgraph.req4(dgraph_client)
        elif option == 9:
            print("\nHábitos compartidos entre amigos")
            model_dgraph.req5(dgraph_client)
        elif option == 0:
            break
        else:
            print("Opción no válida, intenta de nuevo.")


def events_menu(cassandra_session, mongo_db, dgraph_client):
    options = {
        1: "Historial de actividad reciente del usuario",
        2: "Actividad en un día específico",
        3: "Usuarios con registros en un día específico",
        4: "Eventos recomendados por categoría y amigos",
    }

    while True:
        print_submenu("Eventos y actividad", options)
        try:
            option = int(input("\nSelecciona una opción: "))
        except ValueError:
            print("Por favor ingresa un número válido.")
            continue

        if option == 1:
            print("\nHistorial de actividad reciente del usuario")
            model_cassandra.req1(cassandra_session)
        elif option == 2:
            print("\nActividad en un día específico")
            model_cassandra.req2(cassandra_session)
        elif option == 3:
            print("\nUsuarios con registros en un día específico")
            model_mongo.req7(mongo_db)
        elif option == 4:
            print("\nEventos recomendados por categoría y amigos")
            model_dgraph.req8(dgraph_client)
        elif option == 0:
            break
        else:
            print("Opción no válida, intenta de nuevo.")


def profile_menu(cassandra_session, mongo_db, dgraph_client):
    options = {
        1: "Registros de estado de ánimo",
        2: "Usuarios con más de 5 hábitos",
        3: "Usuarios con bajo rendimiento",
        4: "Distribución de estados",
        5: "Logros e insignias por usuario",
        6: "Amigos con intereses similares",
        7: "Usuarios influyentes",
    }

    while True:
        print_submenu("Bienestar y perfil del usuario", options)
        try:
            option = int(input("\nSelecciona una opción: "))
        except ValueError:
            print("Por favor ingresa un número válido.")
            continue

        if option == 1:
            print("\nRegistros de estado de ánimo")
            model_cassandra.req5(cassandra_session)
        elif option == 2:
            print("\nUsuarios con más de 5 hábitos")
            model_mongo.req2(mongo_db)
        elif option == 3:
            print("\nUsuarios con bajo rendimiento")
            model_mongo.req5(mongo_db)
        elif option == 4:
            print("\nDistribución de estados")
            model_mongo.req8(mongo_db)
        elif option == 5:
            print("\nLogros e insignias por usuario")
            model_mongo.req9(mongo_db)
        elif option == 6:
            print("\nAmigos con intereses similares")
            model_dgraph.req2(dgraph_client)
        elif option == 7:
            print("\nUsuarios influyentes")
            model_dgraph.req3(dgraph_client)
        elif option == 0:
            break
        else:
            print("Opción no válida, intenta de nuevo.")


def routines_menu(cassandra_session, mongo_db, dgraph_client):
    options = {
        1: "Próximos recordatorios del día",
        2: "Mostrar rutina por categoría",
    }

    while True:
        print_submenu("Rutinas y recordatorios", options)
        try:
            option = int(input("\nSelecciona una opción: "))
        except ValueError:
            print("Por favor ingresa un número válido.")
            continue

        if option == 1:
            print("\nPróximos recordatorios del día")
            model_cassandra.req4(cassandra_session)
        elif option == 2:
            print("\nMostrar rutina por categoría")
            model_cassandra.req6(cassandra_session)
        elif option == 0:
            break
        else:
            print("Opción no válida, intenta de nuevo.")


def challenges_menu(cassandra_session, mongo_db, dgraph_client):
    options = {
        1: "Retos activos en participación por usuario",
        2: "Top de participantes por reto",
        3: "Retos recomendados por afinidad social",
    }

    while True:
        print_submenu("Retos", options)
        try:
            option = int(input("\nSelecciona una opción: "))
        except ValueError:
            print("Por favor ingresa un número válido.")
            continue

        if option == 1:
            print("\nRetos activos en participación por usuario")
            model_cassandra.req7(cassandra_session)
        elif option == 2:
            print("\nTop de participantes por reto")
            model_cassandra.req8(cassandra_session)
        elif option == 3:
            print("\nRetos recomendados por afinidad social")
            model_dgraph.req6(dgraph_client)
        elif option == 0:
            break
        else:
            print("Opción no válida, intenta de nuevo.")


def social_menu(cassandra_session, mongo_db, dgraph_client):
    options = {
        1: "Actividades recomendadas por categorías",
    }

    while True:
        print_submenu("Social y recomendaciones", options)
        try:
            option = int(input("\nSelecciona una opción: "))
        except ValueError:
            print("Por favor ingresa un número válido.")
            continue

        if option == 1:
            print("\nActividades recomendadas por categorías")
            model_dgraph.req7(dgraph_client)
        elif option == 0:
            break
        else:
            print("Opción no válida, intenta de nuevo.")


# =========================
# MAIN
# =========================

def main():
    print("Iniciando conexiones...")
    conexion = init_connections()
    print("¡Conexiones establecidas!")

    cassandra_session = conexion["cassandra_session"]
    mongo_db = conexion.get("mongo_db")
    dgraph_client = conexion["dgraph_client"]

    print("\n=== Cargando Dgraph ===")
    model_dgraph.drop_all(dgraph_client)
    model_dgraph.create_schema(dgraph_client)
    model_dgraph.load_data(dgraph_client)

    print("\n=== Cargando  Cassandra ===")
    model_cassandra.create_schema(cassandra_session)
    model_cassandra.clear_data(cassandra_session)
    model_cassandra.load_data(cassandra_session)

    print("\n=== Cargando Mongo ===")
    if mongo_db is None:
        print("MongoDB no está disponible en la conexión; saltando carga de Mongo.")
    else:
        model_mongo.create_database_and_collections(mongo_db)
        model_mongo.create_indexes(mongo_db)
        model_mongo.load_data(mongo_db)

    print("¡Schemas y datos cargados correctamente!")

    while True:
        print_main_menu()
        try:
            option = int(input("\nSelecciona una funcionalidad: "))
        except ValueError:
            print("Por favor ingresa un número válido.")
            continue

        if option == 1:
            habits_menu(cassandra_session, mongo_db, dgraph_client)
        elif option == 2:
            events_menu(cassandra_session, mongo_db, dgraph_client)
        elif option == 3:
            profile_menu(cassandra_session, mongo_db, dgraph_client)
        elif option == 4:
            routines_menu(cassandra_session, mongo_db, dgraph_client)
        elif option == 5:
            challenges_menu(cassandra_session, mongo_db, dgraph_client)
        elif option == 6:
            social_menu(cassandra_session, mongo_db, dgraph_client)
        elif option == 0:
            print("\nCerrando conexiones...")
            close_connections(conexion)
            print("Adiós master")
            break
        else:
            print("Opción no válida, intenta de nuevo.")


if __name__ == '__main__':
    main()
