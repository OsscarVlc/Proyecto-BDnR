import csv
import json
import pydgraph

def drop_all(client):
    client.alter(pydgraph.Operation(drop_all=True))


# Crear el esquema
def create_schema(client):
    """
    - Crear un esquema que contenga al menos 4 tipos de nodos.
    - Incluir:
        * Un campo de texto indexado.
        * Un campo numérico indexado.
        * Un campo de tipo geo.
        * Al menos una relación con dirección inversa (@reverse).
    - Usa client.alter(pydgraph.Operation(schema=schema_string))
    """

    #--------------------------------------------------------------------------------------------------------------
    #//////////////////////////////////////////////////////////////////////////////////////////////////////////////
    schema = """
    name: string @index(term, exact) .

    category_name: string @index(term) .

    activity_name: string .

    habit_name: string .

    event_name: string .
    event_date: datetime .

    challenge_name: string .

    usuario_habito: [uid] @reverse .
    usuario_actividad: [uid] .
    sigue_a: [uid] @reverse .
    usuario_reto: [uid] @reverse .
    usuario_evento: [uid] .
    evento_categoria: uid .
    actividad_categoria: uid .

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
    #//////////////////////////////////////////////////////////////////////////////////////////////////////////////
    #--------------------------------------------------------------------------------------------------------------

    client.alter(pydgraph.Operation(schema=schema))
    print("Esquema creado\n")

# Cargar Datos
def load_data(client):
    txn = client.txn()
    try:
        data = []

        #-----------------------------------------------------------------------
        # NODOS 
        #-----------------------------------------------------------------------

        NODOS = "data/Dgraph/nodos/"
        RELS  = "data/Dgraph/relaciones/"

        with open(NODOS + "actividades.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["activity_name"],
                    "dgraph.type": "Actividad",
                    "activity_name": row["activity_name"],
                })
                
        with open(NODOS + "categorias.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["category_name"],
                    "dgraph.type": "Categoria",
                    "category_name": row["category_name"],
                })
        
        with open(NODOS + "eventos.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["event_name"],
                    "dgraph.type": "Evento",
                    "event_name": row["event_name"],
                    "event_date": row["event_date"],
                })

        with open(NODOS + "habitos.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["habit_name"],
                    "dgraph.type": "Habito",
                    "habit_name": row["habit_name"],
                })

        with open(NODOS + "retos.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["challenge_name"],
                    "dgraph.type": "Reto",
                    "challenge_name": row["challenge_name"],
                })

        with open(NODOS + "usuarios.csv") as f:
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

        with open(RELS + "actividad_categoria.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["activity_name"],
                    "actividad_categoria": {
                        "uid": "_:" + row["category_name"]
                    }
                })

        with open(RELS + "evento_categoria.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["event_name"],
                    "evento_categoria": {
                        "uid": "_:" + row["category_name"]
                    }
                })

        with open(RELS + "usuario_habito.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["name"],
                    "usuario_habito": {
                        "uid": "_:" + row["habit_name"]
                    }
                })

        with open(RELS + "usuario_actividad.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["name"],
                    "usuario_actividad": {
                        "uid": "_:" + row["activity_name"]
                    }
                })

        with open(RELS + "usuario_evento.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["name"],
                    "usuario_evento": {
                        "uid": "_:" + row["event_name"]
                    }
                })
            
        with open(RELS + "usuario_reto.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append({
                    "uid": "_:" + row["name"],
                    "usuario_reto": {
                        "uid": "_:" + row["challenge_name"]
                    }
                })

        with open(RELS + "sigue_a.csv") as f:
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

# Queries
def req1(client):
    usuario = input("Usuario: ")

    query = """
    query q($usuario: string){
        var(func: type(Usuario)) @filter(eq(name, $usuario)){
            habito_mio as usuario_habito
            sigue_a{
                habito_no_mio as usuario_habito
            }
        }

        recomendacion(func: uid(habito_no_mio)) @filter(NOT uid(habito_mio)){
            habit_name
        }
    }
    """

    # le pongo etiqueta al atributo para diferenciarlos en la comparacion de uid
    # aca en vez de filtrar por el tipo de Habito nos vamos directamente a la lista de 
    # los habitos de las personas que sigo para compararlos con los del usuario

    # ayuda de chat para imprimir solo lo recomendado
    res = client.txn(read_only=True).query(query, variables={'$usuario': usuario})
    resultado = json.loads(res.json)
    print(json.dumps(resultado["recomendacion"], indent=2))

def req2(client):
    usuario = input("Usuario: ")

    query = """
    query q($usuario: string){
        user(func: type(Usuario)) @filter(eq(name, $usuario)){
            mis_habitos as usuario_habito
        }

        todos(func: type(Usuario)) @filter(NOT eq(name, $usuario)){
            habitos_en_comun as count(usuario_habito @filter(uid(mis_habitos)))
        }

        recomendacion(func: type(Usuario)) @filter(NOT eq(name, $usuario) AND ge(val(habitos_en_comun), 3)){
            name
            seguidores: count(~sigue_a)
        }
    }
    """

    res = client.txn(read_only=True).query(query, variables={'$usuario': usuario})
    resultado = json.loads(res.json)
    print(json.dumps(resultado["recomendacion"], indent=2))

def req3(client):
    query = """
    {
        famosos(func: type(Usuario)) @filter (ge(count(~sigue_a), 10)){
            name
            seguidores: ~sigue_a{
                seguidor: name
            }
            total_seguidores: count(~sigue_a)
        }
    }
    """

    res = client.txn(read_only=True).query(query)
    print(json.dumps(json.loads(res.json), indent=2))

def req4(client):
    query = """
    query q(){
        habitosconteo(func: type(Habito)){
            total as count(~usuario_habito)
        }

        habitos(func: type(Habito), orderdesc: val(total), first: 5){
            habit_name
            total_usuarios: val(total)
        }
    }
    """
    res = client.txn(read_only=True).query(query)
    resultado = json.loads(res.json)
    print(json.dumps(resultado["habitos"], indent=2))

def req5(client):
    usuario= input("Usuario: ")

    query= """
    query q($usuario: string){
        user(func: type(Usuario)) @filter (eq(name, $usuario)){
            mis_habitos as usuario_habito
            yo_sigo as sigue_a
            me_siguen as ~sigue_a
        }

        amigos(func: uid(yo_sigo)) @filter (uid(me_siguen)){
            name
            habitos_comun: usuario_habito @filter(uid(mis_habitos)){
                habit_name
            }
            total_comun: count(usuario_habito @filter(uid(mis_habitos)))
        }
    }
    """
    res = client.txn(read_only=True).query(query, variables={'$usuario': usuario})
    resultado = json.loads(res.json)
    print(json.dumps(resultado["amigos"], indent=2))

def req6(client):
    usuario= input("Usuario: ")

    query = """
    query q($usuario: string){
        var(func: type(Usuario)) @filter(eq(name, $usuario)){
            mis_retos as usuario_reto
            mis_habitos as usuario_habito
            yo_sigo as sigue_a
        }

        habitos(func: uid(yo_sigo)){
            habitos_en_comun as count(usuario_habito @filter(uid(mis_habitos)))
        }

        retos(func: uid(yo_sigo)) @filter(ge(val(habitos_en_comun), 2)){
            retos_amigos as usuario_reto @filter(NOT uid(mis_retos))
        }

        recomendacion(func: uid(retos_amigos)){
            challenge_name
            total_participantes: count(~usuario_reto)
        }
    }
    """

    res = client.txn(read_only=True).query(query, variables={'$usuario': usuario})
    resultado = json.loads(res.json)
    print(json.dumps(resultado["recomendacion"], indent=2))

def req7(client): 
    usuario = input("Usuario: ")

    query = """
    query q($usuario: string){
        usuarioact(func: type(Usuario)) @filter(eq(name, $usuario)){
            mis_actividades as usuario_actividad
        }

        usuariocat(func: type(Usuario)) @filter(eq(name, $usuario)){
            usuario_actividad{
                categorias_interes as actividad_categoria
            }
        }

        recomendacion(func: type(Actividad)) @filter(uid_in(actividad_categoria, uid(categorias_interes)) AND NOT uid(mis_actividades)){
            nombre_actividad: activity_name
            categoria: actividad_categoria {
                nombre_categoria: category_name
            }
        }
    }
    """

    # el uid_in nos ayuda a buscar por id pero en relaciones que tienen que ver con el nodo, a diferecia de los otros
    # que comparabamos con el uid dentro del nodo

    res = client.txn(read_only=True).query(query, variables={'$usuario': usuario})
    resultado = json.loads(res.json)
    print(json.dumps(resultado["recomendacion"], indent=2))

def req8(client):
    usuario = input("Usuario: ")
    categoria = input("Categoría: ")

    query = """
    query q($usuario: string, $categoria: string){
        usuario(func: type(Usuario)) @filter(eq(name, $usuario)){
            mis_eventos as usuario_evento
            sigue_a {
                eventos_amigos as usuario_evento
            }
        }

        categoria(func: type(Categoria)) @filter(eq(category_name, $categoria)){
            categiria_uid as uid
        }

        recomendacion(func: uid(eventos_amigos)) @filter(NOT uid(mis_eventos) AND uid_in(evento_categoria, uid(categiria_uid))){
            nombre_evento: event_name
            fecha: event_date
            categoria: evento_categoria {
                nombre_categoria: category_name
            }
        }
    }
    """

    res = client.txn(read_only=True).query(query, variables={'$usuario': usuario, '$categoria': categoria})
    resultado = json.loads(res.json)
    print(json.dumps(resultado["recomendacion"], indent=2))


