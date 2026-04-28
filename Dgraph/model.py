import csv
import json
import pydgraph

def create_schema(client):
    """
    #usuario
    name: string @index(term, exact) .

    #categoria
    category_name: string @index(term) .

    #actividad
    activity_name: string @index(term) .

    #habito
    habit_name: string @index(term) .

    #evento: 
    event_name: string @index(term) .
    event_date: datetime .

    #reto
    challenge_name: string @index(term) .

    #relaciones
    usuario_habito: [uid] @reverse .
    usuario_actividad: [uid] @reverse .
    sigue_a: [uid] @reverse .
    usuario_reto: [uid] @reverse .
    usuario_evento: [uid] @reverse .
    evento_categoria: uid @reverse .
    actividad_categoria: uid @reverse .

    #tipos
    type Usuario {
        name
        sigue_a
        usuario_habito
        usuario_actividad
        usuario_evento
        usuario_reto
    }

    type Categoria {
        category_name

    }

    type Actividad {
        activity_name
        actividad_categoria
    }

    type Habito {
        habit_name
        
    }

    type Evento {
        event_name
        event_date
        evento_categoria

    }

    type Reto {
        challenge_name
    }
    """

    client.alter(pydgraph.Operation(schema=schema))
    print("Esquema creado\n")

def load_data(client):
    """
    - Cargar datos desde archivos CSV y crear nodos.
    - Crear relaciones entre nodos
    - Usar transacciones y mutations.
    - Ejemplo de estructura:
        txn = client.txn()
        txn.mutate(set_obj=lista_de_objetos)
        txn.commit()
    """
    txn = client.txn()
    try:
        data = []

        #-----------------------------------------------------------------------
        # NODOS 
        #-----------------------------------------------------------------------

        with open("actividades.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["activity_name"],
                    "dgraph.type": "Actividad",
                    "activity_name": row["activity_name"],
                })
                
        with open("categorias.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["category_name"],
                    "dgraph.type": "Categoria",
                    "category_name": row["category_name"],
                })
        
        with open("eventos.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["event_name"],
                    "dgraph.type": "Evento",
                    "event_name": row["event_name"],
                    "event_date": row["event_date"],
                })

        with open("habitos.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["habit_name"],
                    "dgraph.type": "Habito",
                    "habit_name": row["habit_name"],
                })

        with open("retos.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["challenge_name"],
                    "dgraph.type": "Reto",
                    "challenge_name": row["challenge_name"],
                })

        with open("usuarios.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["name"],
                    "dgraph.type": "Usuario",
                    "name": row["name"],
                })                

        #-----------------------------------------------------------------------
        # RELACIONES 
        #-----------------------------------------------------------------------

        with open("actividad_categoria.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["activity_name"],
                    "actividad_categoria": {
                        "uid": "_:" + row["category_name"]
                    }
                })

        with open("evento_categoria.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["event_name"],
                    "evento_categoria": {
                        "uid": "_:" + row["category_name"]
                    }
                })

        with open("usuario_habito.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["name"],
                    "usuario_habito": {
                        "uid": "_:" + row["habit_name"]
                    }
                })

        with open("usuario_actividad.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["name"],
                    "usuario_actividad": {
                        "uid": "_:" + row["activity_name"]
                    }
                })

        with open("usuario_evento.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["name"],
                    "usuario_evento": {
                        "uid": "_:" + row["event_name"]
                    }
                })
            
        with open("usuario_reto.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["name"],
                    "usuario_reto": {
                        "uid": "_:" + row["challenge_name"]
                    }
                })

        with open("sigue_a.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["name"],
                    "sigue_a": {
                        "uid": "_:" + row["followed_name"]
                    }
                })

        res = txn.mutate(set_obj=data)
        txn.commit()
        print("Datos cargados correctamente \n")
        return res.uids

    finally:
        txn.discard()

