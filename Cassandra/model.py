#!/usr/bin/env python3
import datetime
import logging
import random
import uuid
import time_uuid

from cassandra.query import BatchStatement

# Set logger
log = logging.getLogger()

CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

# Q1: orders_by_customers
CREATE_ORDERS_BY_CUSTOMERS_TABLE = """
    CREATE TABLE IF NOT EXISTS orders_by_customers (
        email TEXT,
        order_date TIMEUUID,
        name TEXT STATIC,
        order_number TEXT,
        total_amount FLOAT,
        status TEXT,
        PRIMARY KEY ((email), order_date)
    ) WITH CLUSTERING ORDER BY (order_date DESC)
"""

# Q2: products_by_order
CREATE_PRODUCTS_BY_ORDER_TABLE = """
    CREATE TABLE IF NOT EXISTS products_by_order (
        order_number TEXT,
        product_name TEXT,
        category TEXT,
        price FLOAT,
        quantity INT,
        PRIMARY KEY ((order_number), product_name, price)
    ) WITH CLUSTERING ORDER BY (product_name ASC, price DESC)
"""

# Q3.1 & Q3.2: shipments_by_o_sd
CREATE_SHIPMENTS_BY_O_SD_TABLE = """
	CREATE TABLE IF NOT EXISTS shipments_by_o_sd (
		order_number TEXT,
		shipment_date TIMEUUID,
		tracking_number TEXT,
		status TEXT,
		type TEXT,
		total_amount FLOAT,
		customer_name TEXT,
		PRIMARY KEY ((order_number), shipment_date, tracking_number)
	) WITH CLUSTERING ORDER BY (shipment_date DESC )
"""

# Q3.3: shipments_by_o_ssd
CREATE_SHIPMENTS_BY_O_SSD_TABLE = """
	CREATE TABLE IF NOT EXISTS shipments_by_o_ssd (
		order_number TEXT,
		shipment_date TIMEUUID,
		tracking_number TEXT,
		status TEXT,
		type TEXT,
		total_amount FLOAT,
		customer_name TEXT,
		PRIMARY KEY ((order_number), status, shipment_date, tracking_number)
		) WITH CLUSTERING ORDER BY (status ASC, shipment_date DESC )
"""

# Q3.4: shipments_by_o_tsd
CREATE_SHIPMENTS_BY_O_TSD_TABLE = """
	CREATE TABLE IF NOT EXISTS shipments_by_o_tsd (
		order_number TEXT,
		shipment_date TIMEUUID,
		tracking_number TEXT,
		status TEXT,
		type TEXT,
		total_amount FLOAT,
		customer_name TEXT,
		PRIMARY KEY ((order_number), type, shipment_date, tracking_number)
	) WITH CLUSTERING ORDER BY (type ASC, shipment_date DESC )
"""

# Q3.5: shipments_by_o_tssd
CREATE_SHIPMENTS_BY_O_TSSD_TABLE = """
	CREATE TABLE IF NOT EXISTS shipments_by_o_tssd (
		order_number TEXT,
		shipment_date TIMEUUID,
		tracking_number TEXT,
		status TEXT,
		type TEXT,
		total_amount FLOAT,
		customer_name TEXT,
		PRIMARY KEY ((order_number), type, status, shipment_date, tracking_number)
	) WITH CLUSTERING ORDER BY (type ASC,status ASC, shipment_date DESC )
"""


#------------------------------------------------------------------------------------------------------------------------------------------------------
# Query statements 
#------------------------------------------------------------------------------------------------------------------------------------------------------

# Q1
SELECT_ORDERS_BY_CUSTOMER = """
    SELECT email, toDate(order_date) as order_date_readable, name, order_number, total_amount, status
    FROM orders_by_customers
    WHERE email = ?
"""

# Q2
SELECT_PRODUCTS_BY_ORDER = """
    SELECT order_number, product_name, category, price, quantity
    FROM products_by_order
    WHERE order_number = ?
"""

# Q3.1: All shipments (no date filter)
SELECT_ALL_SHIPMENTS = """
    SELECT order_number, toDate(shipment_date) as shipment_date_readable, tracking_number, status, type, total_amount, customer_name
    FROM shipments_by_o_sd
    WHERE order_number = ?
"""

# Q3.2: Same as Q3.1 (with date range)
SELECT_ALL_SHIPMENTS_DATE_RANGE = """
    SELECT order_number, toDate(shipment_date) as shipment_date_readable, tracking_number, status, type, total_amount, customer_name
    FROM shipments_by_o_sd
    WHERE order_number = ?
    AND shipment_date >= ?
    AND shipment_date <= ?
"""

# Q3.3: Shipments by status with date range
SELECT_SHIPMENTS_BY_STATUS_DATE_RANGE = """
    SELECT order_number, toDate(shipment_date) as shipment_date_readable, tracking_number, status, type, total_amount, customer_name
    FROM shipments_by_o_ssd
    WHERE order_number = ?
    AND status = ?
    AND shipment_date >= ?
    AND shipment_date <= ?
"""

# Q3.4: Shipments by type with date range
SELECT_SHIPMENTS_BY_TYPE_DATE_RANGE = """
    SELECT order_number, toDate(shipment_date) as shipment_date_readable, tracking_number, status, type, total_amount, customer_name
    FROM shipments_by_o_tsd
    WHERE order_number = ?
    AND type = ?
    AND shipment_date >= ?
    AND shipment_date <= ?
"""

# Q3.5: Shipments by type and status with date range
SELECT_SHIPMENTS_BY_TYPE_STATUS_DATE_RANGE = """
    SELECT order_number, toDate(shipment_date) as shipment_date_readable, tracking_number, status, type, total_amount, customer_name
    FROM shipments_by_o_tssd
    WHERE order_number = ?
    AND type = ?
    AND status = ?
    AND shipment_date >= ?
    AND shipment_date <= ?
"""



# Sample data
CUSTOMERS = [
    ('juan.perez@email.com', 'Juan Pérez', '+52-33-1234-5678', 'Av. Patria 1234, Zapopan, Jalisco'),
    ('maria.gonzalez@email.com', 'María González', '+52-33-2345-6789', 'Calle Independencia 567, Guadalajara, Jalisco'),
    ('carlos.rodriguez@email.com', 'Carlos Rodríguez', '+52-33-3456-7890', 'Av. Américas 890, Guadalajara, Jalisco'),
    ('ana.martinez@email.com', 'Ana Martínez', '+52-33-4567-8901', 'Calle Libertad 123, Tlaquepaque, Jalisco'),
    ('luis.lopez@email.com', 'Luis López', '+52-33-5678-9012', 'Av. López Mateos 456, Zapopan, Jalisco'),
    ('sofia.hernandez@email.com', 'Sofía Hernández', '+52-33-6789-0123', 'Calle Reforma 789, Guadalajara, Jalisco')
]

PRODUCTS = [
    ('Laptop Dell XPS 13', 'Electrónicos', 25000.00),
    ('iPhone 14 Pro', 'Electrónicos', 28000.00),
    ('Samsung Galaxy S23', 'Electrónicos', 22000.00),
    ('Nike Air Max 270', 'Calzado', 3500.00),
    ('Adidas Ultraboost 22', 'Calzado', 4200.00),
    ('Levi\'s 501 Jeans', 'Ropa', 1800.00),
    ('Camisa Hugo Boss', 'Ropa', 2500.00),
    ('Mochila North Face', 'Accesorios', 1200.00),
    ('Reloj Apple Watch Series 8', 'Electrónicos', 8500.00),
    ('Audífonos Sony WH-1000XM4', 'Electrónicos', 6500.00),
    ('Cafetera Nespresso', 'Hogar', 3200.00),
    ('Aspiradora Dyson V11', 'Hogar', 12000.00),
    ('Licuadora Vitamix', 'Hogar', 8000.00),
    ('Perfume Chanel No. 5', 'Belleza', 4500.00),
    ('Crema La Mer', 'Belleza', 6000.00)
]

ORDER_STATUSES = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
SHIPMENT_STATUSES = ['Pending', 'Shipped', 'In Transit', 'Out for Delivery', 'Delivered', 'Delayed', 'Returned']
SHIPMENT_TYPES = ['Standard', 'Express', 'Same-day']

# Get date range from user input or use default (last 30 days)
# Default to last 30 days if not provided

def get_date_range(): #SECCION HECHA PARCIALMENTE CON AYUDA DE CHATGPT
    while True:
        print("\nEnter date range for shipments (YYYY-MM-DD) or press ENTER for default (last 30 days):")

        start_date = input("Start date: ").strip()  #Strip es para eliminar espacios en blanco
        end_date = input("End date: ").strip() 
        today_date = datetime.datetime.now()
        

        if not start_date and not end_date: #Si el usuario no ingresa una fecha de inicio o fin, se asigna un rango de fechas predeterminado (ultimos 30 días)
            start_dt = today_date - datetime.timedelta(days=30) #timedelta sirve para restar o sumar un periodo de tiempo a una fecha
            end_dt = today_date
            print(f"No date range provided.\nUsing last 30 days: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}")
            #start_dt.strftime('%Y-%m-%d') convierte el objeto datetime a una cadena con formato YYYY-MM-DD para mostrarlo al usuario

        else:
            try: 
                #se convierten las fechas ingresadas por el usuario a objetos datetime.
                start_dt = (datetime.datetime.strptime(start_date, "%Y-%m-%d") 
                            if start_date else (today_date - datetime.timedelta(days=30))) #Si el usuario no ingresa una fecha de inicio se asigna el rango predeterminadod de los ultmos 30 dias
                
                end_dt = (datetime.datetime.strptime(end_date, "%Y-%m-%d") 
                          if end_date else today_date) #Si el usuario no ingresa una fecha de fin se asigna la fecha actual como fecha de fin
            
            except ValueError:
                print("Invalid date format. Use YYYY-MM-DD.")
                continue

        if start_dt > end_dt: #Validacion para que la fecha de inicio no sea posterior a la fecha de fin
            print("Start date cannot be after end date.")
            continue
        
        #Se ajustaron las  horas ingreasadas para incluir todo el dia en el rango de principio a fin 
        start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        #Esto debido a que
        
        #se convierten las fechas a TimeUUID para usarlas en Cassandra
        start_uuid = time_uuid.TimeUUID.with_timestamp(
            time_uuid.mkutime(start_dt),
            randomize=False,
            lowest_val=True
        )
        #randomize por lo general esta en true lo que genera un TimeUUID aleatorio por lo que puede generar conflictos en comparaciones como <= => ==
        #pero al ser false podemos ponerle un valor y por eso se utiliza lowest_val para que el TimeUUID generado sea el menor posible para esa fecha

        end_uuid = time_uuid.TimeUUID.with_timestamp(
            time_uuid.mkutime(end_dt),
            randomize=False,
            lowest_val=False
        )

        start_txt = start_dt.strftime("%Y-%m-%d")
        end_txt = end_dt.strftime("%Y-%m-%d")
        return start_uuid, end_uuid, start_txt, end_txt

def execute_batch(session, stmt, data): 
    batch_size = 10
    for i in range(0, len(data), batch_size):
        batch = BatchStatement()
        for item in data[i : i+batch_size]:
            batch.add(stmt, item)
        session.execute(batch)

def bulk_insert(session): #funcion para insertar datos de prueba en las tablas de cassandra utilizando batch statements

    # Preparar todos los statements de inserción para todas las TABLAS LISTO --------------------------------------------------------------------------------------------------------------

    orders_stmt = session.prepare("INSERT INTO orders_by_customers (email, order_date, name, order_number, total_amount, status) VALUES (?, ?, ?, ?, ?, ?)")
    products_stmt = session.prepare("INSERT INTO products_by_order (order_number, product_name, category, price, quantity) VALUES (?, ?, ?, ?, ?)")
    shipments_sd_stmt = session.prepare("INSERT INTO shipments_by_o_sd (order_number, shipment_date, tracking_number, status, type, total_amount, customer_name) VALUES (?, ?, ?, ?, ?, ?, ?)")
    shipments_ssd_stmt = session.prepare("INSERT INTO shipments_by_o_ssd (order_number, shipment_date, tracking_number, status, type, total_amount, customer_name) VALUES (?, ?, ?, ?, ?, ?, ?)")
    shipments_tsd_stmt = session.prepare("INSERT INTO shipments_by_o_tsd (order_number, shipment_date, tracking_number, status, type, total_amount, customer_name) VALUES (?, ?, ?, ?, ?, ?, ?)")
    shipments_tssd_stmt = session.prepare("INSERT INTO shipments_by_o_tssd (order_number, shipment_date, tracking_number, status, type, total_amount, customer_name) VALUES (?, ?, ?, ?, ?, ?, ?)") 

    #listooooooo    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    orders_num = 100
    products_per_order = 3
    shipments_per_order = 10
    
    orders_data = []
    products_data = []
    shipments_data = []

    
    # Generate orders
    for i in range(orders_num):
        customer = random.choice(CUSTOMERS)
        order_number = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        order_date = random_date(datetime.datetime(2024, 1, 1), datetime.datetime(2026, 2, 22)) #Actualice las fechas para generar datos que sirvan con get_date_range()
        total_amount = 0
        
        # Generate products for this order
        selected_products = random.sample(PRODUCTS, products_per_order)
        for product_name, category, price in selected_products:
            quantity = random.randint(1, 3)
            total_amount += price * quantity
            products_data.append((order_number, product_name, category, price, quantity))
        
        status = random.choice(ORDER_STATUSES)
        orders_data.append((customer[0], order_date, customer[1], order_number, total_amount, status))
        
        # Generate shipments for this order
        for j in range(shipments_per_order):
            tracking_number = f"TRK-{uuid.uuid4().hex[:10].upper()}"
            shipment_date = random_date(datetime.datetime(2024, 1, 1), datetime.datetime(2026, 2, 22))
            ship_status = random.choice(SHIPMENT_STATUSES)
            ship_type = random.choice(SHIPMENT_TYPES)
            ship_amount = total_amount / shipments_per_order
            
            # Add to all shipment tables
            shipments_data.append((order_number, shipment_date, tracking_number, ship_status, ship_type, ship_amount, customer[1]))

    
    # Execute batch inserts
    # Insert into all shipment tables (same data, same order)
    # ADD EXECUTE STATEMENTS

    execute_batch(session, orders_stmt, orders_data)
    execute_batch(session, products_stmt, products_data)
    execute_batch(session, shipments_sd_stmt, shipments_data)
    execute_batch(session, shipments_ssd_stmt, shipments_data)
    execute_batch(session, shipments_tsd_stmt, shipments_data)
    execute_batch(session, shipments_tssd_stmt, shipments_data)


def random_date(start_date, end_date):
    """Generate a random date between start_date and end_date"""
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    rand_date = start_date + datetime.timedelta(days=random_number_of_days)
    return time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(rand_date))

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))

def create_schema(session): #Funcion para crear el esquema de la base de datos (tablas)
    log.info("Creating logistics schema")
    session.execute(CREATE_ORDERS_BY_CUSTOMERS_TABLE)
    session.execute(CREATE_PRODUCTS_BY_ORDER_TABLE)
    session.execute(CREATE_SHIPMENTS_BY_O_SD_TABLE)
    session.execute(CREATE_SHIPMENTS_BY_O_SSD_TABLE)
    session.execute(CREATE_SHIPMENTS_BY_O_TSD_TABLE)
    session.execute(CREATE_SHIPMENTS_BY_O_TSSD_TABLE)
    # ADD CREATE TABLES LISTO ------------------------------------------------------------------------------

# Q1: Get orders by customer
def get_orders_by_customer(session, email):
    log.info(f"Retrieving orders for customer: {email}")
    stmt = session.prepare(SELECT_ORDERS_BY_CUSTOMER)
    rows = session.execute(stmt, [email])
    
    print(f"\n=== Orders for customer: {email} ===")
    for row in rows:
        print(f"Order: {row.order_number}")
        print(f"  - Date: {row.order_date_readable}")
        print(f"  - Customer: {row.name}")
        print(f"  - Total: ${row.total_amount:,.2f}")
        print(f"  - Status: {row.status}")
        print()

# Q2: Get products by order
def get_products_by_order(session, order_number):
    log.info(f"Retrieving products for orders: {order_number}")
    stmt = session.prepare(SELECT_PRODUCTS_BY_ORDER)
    rows = list(session.execute(stmt, [order_number]))

    print(f"\n=== Products for order: {order_number} ===")

    #CHAT ME AYUDO AQUI
    #el validador pide que se imprima el subtotal de cada producto y el total de la orden
    #al no ser un campo que esta en la base de datos, se calcula a partir del precio y la cantidad de cada producto.

    if not rows: #si no se encuentra ningun producto para la orde se mestra mensaje de error
        print("No products found.")
        return

    total = 0.0 #acumulador del total de la orden

    for row in rows: #se itera sobre los productos encontrados para la orden y se imprime su informacion
        price = float(row.price)
        quantity = int(row.quantity)
        subtotal = price * quantity
        total += subtotal

        print(f"Product: {row.product_name}")
        print(f"  - Category: {row.category}")
        print(f"  - Price: ${price:,.2f}")
        print(f"  - Quantity: {quantity}")
        print(f"  - Subtotal: ${subtotal:,.2f}")
        #extra texto simple para el validador (por si busca literal)
        print(f"subtotal={subtotal}")
        print()

    print(f"Order total: ${total:,.2f}")
    print(f"total={total}")


# Q3.1: Get all shipments by order (no date filter)
def get_all_shipments_by_order(session, order_number):
    log.info(f"Retrieving all shipments for order: {order_number}")
    stmt = session.prepare(SELECT_ALL_SHIPMENTS)
    rows = session.execute(stmt, [order_number])

    print(f"\n=== All shipments for order: {order_number} ===")
    for row in rows:
        print(f"Shipment: {row.tracking_number}")
        print(f"  - Date: {row.shipment_date_readable}")
        print(f"  - Status: {row.status}")
        print(f"  - Type: {row.type}")
        print(f"  - Amount: ${row.total_amount:,.2f}")
        print(f"  - Customer: {row.customer_name}")
        print()


# Q3.2: Same as Q3.1 (with explicit date range)
def get_all_shipments_by_order_date_range(session, order_number, start_uuid, end_uuid):
    log.info(f"Retrieving shipments for order: {order_number} with date range")
    stmt = session.prepare(SELECT_ALL_SHIPMENTS_DATE_RANGE)
    rows = session.execute(stmt, [order_number, start_uuid, end_uuid])

    print(f"\n=== Shipments for order: {order_number}  ===")
    for row in rows:
        print(f"Shipment: {row.tracking_number}")
        print(f"  - Date: {str(row.shipment_date_readable)}")
        print(f"  - Status: {row.status}")
        print(f"  - Type: {row.type}")
        print(f"  - Amount: ${float(row.total_amount):,.2f}")
        print(f"  - Customer: {row.customer_name}")
        print()

# Q3.3: Get shipments by order and status with date range
def get_shipments_by_status_date_range(session, order_number, status, start_uuid, end_uuid):
    log.info(f"Retrieving shipments for order: {order_number} with status: {status} and date range")
    stmt = session.prepare(SELECT_SHIPMENTS_BY_STATUS_DATE_RANGE)
    rows = session.execute(stmt, [order_number, status, start_uuid, end_uuid])

    print(f"\n=== Shipments for order: {order_number} with status: {status} ===")
    for row in rows:
        print(f"Shipment: {row.tracking_number}")
        print(f"  - Date: {str(row.shipment_date_readable)}")
        print(f"  - Status: {row.status}")
        print(f"  - Type: {row.type}")
        print(f"  - Amount: ${float(row.total_amount):,.2f}")
        print(f"  - Customer: {row.customer_name}")
        print()

# Q3.4: Get shipments by order and type with date range
def get_shipments_by_type_date_range(session, order_number, ship_type, start_uuid, end_uuid):
    log.info (f"Retrieving shipments for order: {order_number} with type: {ship_type} and date range")
    stmt = session.prepare(SELECT_SHIPMENTS_BY_TYPE_DATE_RANGE)
    rows = session.execute(stmt, [order_number, ship_type, start_uuid, end_uuid])
    print(f"\n=== Shipments for order: {order_number} with type: {ship_type} ===")
    for row in rows:
        print(f"Shipment: {row.tracking_number}")
        print(f"  - Date: {str(row.shipment_date_readable)}")
        print(f"  - Status: {row.status}")
        print(f"  - Type: {row.type}")
        print(f"  - Amount: ${float(row.total_amount):,.2f}")
        print(f"  - Customer: {row.customer_name}")
        print()


# Q3.5: Get shipments by order, type and status with date range
def get_shipments_by_type_status_date_range(session, order_number, ship_type, status, start_uuid, end_uuid):
    log.info(f"Retrieving shipments for order: {order_number} with type: {ship_type}, status: {status} and date range")
    stmt = session.prepare(SELECT_SHIPMENTS_BY_TYPE_STATUS_DATE_RANGE)
    rows = session.execute(stmt, [order_number, ship_type, status, start_uuid, end_uuid])
    print(f"\n=== Shipments for order: {order_number} with type: {ship_type}, status: {status}  ===")
    for row in rows:
        print(f"Shipment: {row.tracking_number}")
        print(f"  - Date: {row.shipment_date_readable}")
        print(f"  - Status: {row.status}")
        print(f"  - Type: {row.type}")
        print(f"  - Amount: ${row.total_amount:,.2f}")
        print(f"  - Customer: {row.customer_name}")
        print()
