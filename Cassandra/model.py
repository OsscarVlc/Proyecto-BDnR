#!/usr/bin/env python3
import datetime
import logging
import csv
import os
import time_uuid
from cassandra.query import BatchStatement

# Set logger
log = logging.getLogger()


# Q1: 
CREATE_USER_EVENTS_BY_TIME_TABLE = """
    CREATE TABLE IF NOT EXISTS user_events_by_time (
        user_id INT,
        event_timestamp TIMEUUID,
        event_type TEXT,
        PRIMARY KEY ((user_id), event_timestamp)
    ) WITH CLUSTERING ORDER BY (event_timestamp DESC)
"""

# Q2: 
CREATE_USER_EVENTS_BY_DAY_TABLE = """
    CREATE TABLE IF NOT EXISTS user_events_by_day (
        user_id INT,
        event_date DATE,
        event_timestamp TIMESTAMP,
        event_type TEXT,
        PRIMARY KEY ((user_id, event_date), event_timestamp)
    ) WITH CLUSTERING ORDER BY (event_timestamp DESC)
"""

#Q3:
CREATE_HABIT_TRACKING_BY_USER_TABLE = """
    CREATE TABLE IF NOT EXISTS habit_tracking_by_user (
        user_id INT,
        check_date DATE,
        habit_name TEXT,
        status TEXT,
        completion_time TIMESTAMP,
        notes TEXT,
        PRIMARY KEY ((user_id,habit_name), check_date )
    ) WITH CLUSTERING ORDER BY (check_date DESC)
"""

# Q4:
CREATE_REMINDERS_BY_USER_DAY_STATUS_TABLE = """
    CREATE TABLE IF NOT EXISTS reminders_by_user_day_status (
        user_id INT,
        reminder_date DATE,
        status TEXT,
        scheduled_time TIMESTAMP,
        reminder_name TEXT,
        habit_name TEXT,
        message TEXT,
        PRIMARY KEY ((user_id, reminder_date, status), scheduled_time)
    ) WITH CLUSTERING ORDER BY (scheduled_time ASC)
"""

# Q5:
CREATE_MOOD_BY_USER_TABLE = """
    CREATE TABLE IF NOT EXISTS mood_by_user (
        user_id INT,
        timestamp TIMESTAMP,
        entry_id INT,
        mood_score INT,
        energy_level INT,
        stress_level INT,
        notes TEXT,
        PRIMARY KEY ((user_id), timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC)
"""

# Q6:
CREATE_ROUTINES_BY_USER_CATEGORY_TABLE = """
    CREATE TABLE IF NOT EXISTS routines_by_user_category (
        user_id INT,
        category TEXT,
        routine_id INT,
        name TEXT,
        scheduled_days LIST<TEXT>,
        status TEXT,
        PRIMARY KEY ((user_id, category), routine_id)
    ) WITH CLUSTERING ORDER BY (routine_id ASC)
"""

# Q7:
CREATE_CHALLENGES_BY_USER_STATUS_TABLE = """
    CREATE TABLE IF NOT EXISTS challenges_by_user_status (
        user_id INT,
        status TEXT,
        challenge_id INT,
        challenge_name TEXT,
        start_date DATE,
        end_date DATE,
        progress INT,
        PRIMARY KEY ((user_id, status), challenge_id)
    ) WITH CLUSTERING ORDER BY (challenge_id ASC)
"""

# Q8:
CREATE_LEADERBOARD_BY_CHALLENGE_TABLE = """
    CREATE TABLE IF NOT EXISTS leaderboard_by_challenge (
        challenge_id INT,
        score INT,
        user_id INT,
        completed_actions INT,
        PRIMARY KEY ((challenge_id), score, user_id)
    ) WITH CLUSTERING ORDER BY (score DESC, user_id ASC)
"""


#------------------------------------------------------------------------------------------------------------------------------------------------------
# Query statements 
#------------------------------------------------------------------------------------------------------------------------------------------------------

# Q1: Historial de actividad reciente del usuario
SELECT_USER_EVENTS_BY_TIME = """
    SELECT user_id, event_timestamp, event_type
    FROM user_events_by_time
    WHERE user_id = ?
"""

# Q2: Actividad en un día específico
SELECT_USER_EVENTS_BY_DAY = """
    SELECT user_id, event_date, event_timestamp, event_type
    FROM user_events_by_day
    WHERE user_id = ?
    AND event_date = ?
"""

# Q3: Cumplimiento de hábito por usuario
SELECT_HABIT_TRACKING_BY_USER = """
    SELECT user_id, habit_name, check_date, status, completion_time, notes
    FROM habit_tracking_by_user
    WHERE user_id = ?
    AND habit_name = ?
"""

# Q4: Próximos recordatorios del día
SELECT_REMINDERS_BY_USER_DAY_STATUS = """
    SELECT user_id, reminder_date, status, scheduled_time, reminder_name, habit_name, message
    FROM reminders_by_user_day_status
    WHERE user_id = ?
    AND reminder_date = ?
    AND status = ?
"""

# Q5: Registros de estado de ánimo dentro de un rango
SELECT_MOOD_BY_USER_RANGE = """
    SELECT user_id, timestamp, entry_id, mood_score, energy_level, stress_level, notes
    FROM mood_by_user
    WHERE user_id = ?
    AND timestamp >= ?
    AND timestamp <= ?
"""

# Q6: Mostrar rutina por categoría
SELECT_ROUTINES_BY_USER_CATEGORY = """
    SELECT user_id, category, routine_id, name, scheduled_days, status
    FROM routines_by_user_category
    WHERE user_id = ?
    AND category = ?
"""

# Q7: Retos activos en participación por usuario
SELECT_CHALLENGES_BY_USER_STATUS = """
    SELECT user_id, status, challenge_id, challenge_name, start_date, end_date, progress
    FROM challenges_by_user_status
    WHERE user_id = ?
    AND status = ?
"""

# Q8: Top de participantes por reto
SELECT_LEADERBOARD_BY_CHALLENGE = """
    SELECT challenge_id, score, user_id, completed_actions
    FROM leaderboard_by_challenge
    WHERE challenge_id = ?
"""

#========================================================================
# Schema creation function
#========================================================================

def create_schema(session):

    session.execute(CREATE_USER_EVENTS_BY_TIME_TABLE)
    session.execute(CREATE_USER_EVENTS_BY_DAY_TABLE)
    session.execute(CREATE_HABIT_TRACKING_BY_USER_TABLE)
    session.execute(CREATE_REMINDERS_BY_USER_DAY_STATUS_TABLE)
    session.execute(CREATE_MOOD_BY_USER_TABLE)
    session.execute(CREATE_ROUTINES_BY_USER_CATEGORY_TABLE)
    session.execute(CREATE_CHALLENGES_BY_USER_STATUS_TABLE)
    session.execute(CREATE_LEADERBOARD_BY_CHALLENGE_TABLE)
    print("Esquema CASSANDRA creado correctamente.\n")



#========================================================================
# data
#========================================================================

# Estas funciones estan hechas con ia y son de parsing para ayudan a convertir los datos del CSV al formato adecuado para Cassandra
def parse_date(value):
    return datetime.date.fromisoformat(value.strip())

def parse_timestamp(value):
    return datetime.datetime.fromisoformat(value.strip().replace("T", " "))

def parse_timeuuid(value):
    dt = parse_timestamp(value)
    return time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(dt))


def parse_list_text(value):
    value = value.strip()
    if not value:
        return []
    return [item.strip() for item in value.split("|") if item.strip()]


def execute_batch(session, stmt, data, batch_size=25):
    if not data:
        return
    for i in range(0, len(data), batch_size):
        batch = BatchStatement()
        for row in data[i:i + batch_size]:
            batch.add(stmt, row)
        session.execute(batch)



def load_data(session, base_path="./data/cassandra"):
    log.info("Loading Cassandra data from CSV files")

    print("\nCargando datos de Cassandra...")

    stmt_q1 = session.prepare("""
        INSERT INTO user_events_by_time (user_id, event_timestamp, event_type)
        VALUES (?, ?, ?)
    """)

    stmt_q2 = session.prepare("""
        INSERT INTO user_events_by_day (user_id, event_date, event_timestamp, event_type)
        VALUES (?, ?, ?, ?)
    """)

    stmt_q3 = session.prepare("""
        INSERT INTO habit_tracking_by_user (user_id, check_date, habit_name, status, completion_time, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    """)

    stmt_q4 = session.prepare("""
        INSERT INTO reminders_by_user_day_status
        (user_id, reminder_date, status, scheduled_time, reminder_name, habit_name, message)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    stmt_q5 = session.prepare("""
        INSERT INTO mood_by_user
        (user_id, timestamp, entry_id, mood_score, energy_level, stress_level, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    stmt_q6 = session.prepare("""
        INSERT INTO routines_by_user_category
        (user_id, category, routine_id, name, scheduled_days, status)
        VALUES (?, ?, ?, ?, ?, ?)
    """)

    stmt_q7 = session.prepare("""
        INSERT INTO challenges_by_user_status
        (user_id, status, challenge_id, challenge_name, start_date, end_date, progress)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    stmt_q8 = session.prepare("""
        INSERT INTO leaderboard_by_challenge
        (challenge_id, score, user_id, completed_actions)
        VALUES (?, ?, ?, ?)
    """)

    # Q1
    q1_data = []
    with open(os.path.join(base_path, "user_events_by_time.csv"), newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            q1_data.append((
                int(row["user_id"]),
                parse_timeuuid(row["event_timestamp"]),
                row["event_type"].strip()
            ))
    execute_batch(session, stmt_q1, q1_data)

    # Q2
    q2_data = []
    with open(os.path.join(base_path, "user_events_by_day.csv"), newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            q2_data.append((
                int(row["user_id"]),
                parse_date(row["event_date"]),
                parse_timestamp(row["event_timestamp"]),
                row["event_type"].strip()
            ))
    execute_batch(session, stmt_q2, q2_data)

    # Q3
    q3_data = []
    with open(os.path.join(base_path, "habit_tracking_by_user.csv"), newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            q3_data.append((
                int(row["user_id"]),
                parse_date(row["check_date"]),
                row["habit_name"].strip(),
                row["status"].strip(),
                parse_timestamp(row["completion_time"]),
                row["notes"].strip()
            ))
    execute_batch(session, stmt_q3, q3_data)

    # Q4
    q4_data = []
    with open(os.path.join(base_path, "reminders_by_user_day_status.csv"), newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            q4_data.append((
                int(row["user_id"]),
                parse_date(row["reminder_date"]),
                row["status"].strip(),
                parse_timestamp(row["scheduled_time"]),
                row["reminder_name"].strip(),
                row["habit_name"].strip(),
                row["message"].strip()
            ))
    execute_batch(session, stmt_q4, q4_data)

    # Q5
    q5_data = []
    with open(os.path.join(base_path, "mood_by_user.csv"), newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            q5_data.append((
                int(row["user_id"]),
                parse_timestamp(row["timestamp"]),
                int(row["entry_id"]),
                int(row["mood_score"]),
                int(row["energy_level"]),
                int(row["stress_level"]),
                row["notes"].strip()
            ))
    execute_batch(session, stmt_q5, q5_data)

    # Q6
    q6_data = []
    with open(os.path.join(base_path, "routines_by_user_category.csv"), newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            q6_data.append((
                int(row["user_id"]),
                row["category"].strip(),
                int(row["routine_id"]),
                row["name"].strip(),
                parse_list_text(row["scheduled_days"]),
                row["status"].strip()
            ))
    execute_batch(session, stmt_q6, q6_data)

    # Q7
    q7_data = []
    with open(os.path.join(base_path, "challenges_by_user_status.csv"), newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            q7_data.append((
                int(row["user_id"]),
                row["status"].strip(),
                int(row["challenge_id"]),
                row["challenge_name"].strip(),
                parse_date(row["start_date"]),
                parse_date(row["end_date"]),
                int(row["progress"])
            ))
    execute_batch(session, stmt_q7, q7_data)

    # Q8
    q8_data = []
    with open(os.path.join(base_path, "leaderboard_by_challenge.csv"), newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            q8_data.append((
                int(row["challenge_id"]),
                int(row["score"]),
                int(row["user_id"]),
                int(row["completed_actions"])
            ))
    execute_batch(session, stmt_q8, q8_data)

    print("Datos de Cassandra cargados correctamente.\n")


#Funcion para eliminar los datos de las tablas de Cassandra
def clear_data(session):
    session.execute("TRUNCATE user_events_by_time")
    session.execute("TRUNCATE user_events_by_day")
    session.execute("TRUNCATE habit_tracking_by_user")
    session.execute("TRUNCATE reminders_by_user_day_status")
    session.execute("TRUNCATE mood_by_user")
    session.execute("TRUNCATE routines_by_user_category")
    session.execute("TRUNCATE challenges_by_user_status")
    session.execute("TRUNCATE leaderboard_by_challenge")
    print("Datos de Cassandra eliminados correctamente.\n")



#========================================================================
# Query functions
#========================================================================
#Q1
def get_user_events_by_time(session, user_id, limit=10):
    log.info(f"Retrieving recent events for user: {user_id}")
    stmt = session.prepare(SELECT_USER_EVENTS_BY_TIME)
    rows = session.execute(stmt, [user_id])

    print(f"\n=== Recent events for user: {user_id} ===")
    found = False
    count = 0

    for row in rows:
        if count >= limit:
            break
        found = True
        print(f"Event: {row.event_type} at {row.event_timestamp}")
        count += 1

    if not found:
        print("No results found.\n")


#Q2
def get_user_events_by_day(session, user_id, event_date):
    log.info(f"Retrieving events for user: {user_id} on date: {event_date}")
    stmt = session.prepare(SELECT_USER_EVENTS_BY_DAY)
    rows = session.execute(stmt, [user_id, event_date])

    print(f"\n=== Events for user: {user_id} on date: {event_date} ===")
    found = False

    for row in rows:
        found = True
        print(f"Event: {row.event_type} at {row.event_timestamp}")

    if not found:
        print("No results found.\n")


#Q3
def get_habit_tracking_by_user(session, user_id, habit_name):
    log.info(f"Retrieving habit tracking for user: {user_id} and habit: {habit_name}")
    stmt = session.prepare(SELECT_HABIT_TRACKING_BY_USER)
    rows = session.execute(stmt, [user_id, habit_name])

    print(f"\n=== Habit tracking for user: {user_id} and habit: {habit_name} ===")
    found = False

    for row in rows:
        found = True
        print(f"Check Date: {row.check_date}")
        print(f"Habit: {row.habit_name}")
        print(f"Status: {row.status}")
        print(f"Completion Time: {row.completion_time}")
        print(f"Notes: {row.notes}")
        print()

    if not found:
        print("No results found.\n")


#Q4
def get_reminders_by_user_day_status(session, user_id, reminder_date, status):
    log.info(f"Retrieving reminders for user: {user_id} on date: {reminder_date} with status: {status}")
    stmt = session.prepare(SELECT_REMINDERS_BY_USER_DAY_STATUS)
    rows = session.execute(stmt, [user_id, reminder_date, status])

    print(f"\n=== Reminders for user: {user_id} on date: {reminder_date} with status: {status} ===")
    found = False

    for row in rows:
        found = True
        print(f"Time: {row.scheduled_time}")
        print(f"Reminder: {row.reminder_name}")
        print(f"Habit: {row.habit_name}")
        print(f"Message: {row.message}")
        print(f"Status: {row.status}")
        print()

    if not found:
        print("No results found.\n")


#Q5
def get_mood_by_user_range(session, user_id, start_time, end_time):
    log.info(f"Retrieving mood entries for user: {user_id} from {start_time} to {end_time}")
    stmt = session.prepare(SELECT_MOOD_BY_USER_RANGE)
    rows = session.execute(stmt, [user_id, start_time, end_time])

    print(f"\n=== Mood entries for user: {user_id} from {start_time} to {end_time} ===")
    found = False

    for row in rows:
        found = True
        print(f"Timestamp: {row.timestamp}")
        print(f"Mood Score: {row.mood_score}")
        print(f"Energy Level: {row.energy_level}")
        print(f"Stress Level: {row.stress_level}")
        print(f"Notes: {row.notes}")
        print()

    if not found:
        print("No results found.\n")


#Q6
def get_routines_by_user_category(session, user_id, category):
    log.info(f"Retrieving routines for user: {user_id} and category: {category}")
    stmt = session.prepare(SELECT_ROUTINES_BY_USER_CATEGORY)
    rows = session.execute(stmt, [user_id, category])

    print(f"\n=== Routines for user: {user_id} and category: {category} ===")
    found = False

    for row in rows:
        found = True
        print(f"Routine: {row.name}")
        print(f"Scheduled Days: {row.scheduled_days}")
        print(f"Status: {row.status}")
        print()

    if not found:
        print("No results found.\n")


#Q7
def get_challenges_by_user_status(session, user_id, status):
    log.info(f"Retrieving challenges for user: {user_id} with status: {status}")
    stmt = session.prepare(SELECT_CHALLENGES_BY_USER_STATUS)
    rows = session.execute(stmt, [user_id, status])

    print(f"\n=== Challenges for user: {user_id} with status: {status} ===")
    found = False

    for row in rows:
        found = True
        print(f"Challenge: {row.challenge_name}")
        print(f"Start Date: {row.start_date}")
        print(f"End Date: {row.end_date}")
        print(f"Progress: {row.progress}")
        print()

    if not found:
        print("No results found.\n")


#Q8
def get_leaderboard_by_challenge(session, challenge_id):
    log.info(f"Retrieving leaderboard for challenge: {challenge_id}")
    stmt = session.prepare(SELECT_LEADERBOARD_BY_CHALLENGE)
    rows = session.execute(stmt, [challenge_id])

    print(f"\n=== Leaderboard for challenge: {challenge_id} ===")
    found = False
    position = 1

    for row in rows:
        found = True
        print(f"Position: {position}")
        print(f"User ID: {row.user_id}")
        print(f"Score: {row.score}")
        print(f"Completed Actions: {row.completed_actions}")
        print()
        position += 1

    if not found:
        print("No results found.\n")


#========================================================================
#Queries para el MAIN
#========================================================================

def req1(session):
    user_id = int(input("Ingresa el user_id: "))
    limit = int(input("¿Cuántos eventos recientes quieres ver?: "))
    get_user_events_by_time(session, user_id, limit)


def req2(session):
    user_id = int(input("Ingresa el user_id: "))
    event_date = datetime.datetime.strptime(
        input("Ingresa la fecha (YYYY-MM-DD): "), "%Y-%m-%d"
    ).date()
    get_user_events_by_day(session, user_id, event_date)


def req3(session):
    user_id = int(input("Ingresa el user_id: "))
    habit_name = input("Ingresa el nombre del hábito: ").strip()
    get_habit_tracking_by_user(session, user_id, habit_name)


def req4(session):
    user_id = int(input("Ingresa el user_id: "))
    reminder_date = datetime.datetime.strptime(
        input("Ingresa la fecha (YYYY-MM-DD): "), "%Y-%m-%d"
    ).date()
    status = input("Ingresa el estado del recordatorio: ").strip()
    get_reminders_by_user_day_status(session, user_id, reminder_date, status)


def req5(session):
    user_id = int(input("Ingresa el user_id: "))

    start_time = datetime.datetime.strptime(
        input("Ingresa la fecha inicial (YYYY-MM-DD): "), "%Y-%m-%d"
    ).replace(hour=0, minute=0, second=0)

    end_time = datetime.datetime.strptime(
        input("Ingresa la fecha final (YYYY-MM-DD): "), "%Y-%m-%d"
    ).replace(hour=23, minute=59, second=59)

    get_mood_by_user_range(session, user_id, start_time, end_time)


def req6(session):
    user_id = int(input("Ingresa el user_id: "))
    category = input("Ingresa la categoría: ").strip()
    get_routines_by_user_category(session, user_id, category)


def req7(session):
    user_id = int(input("Ingresa el user_id: "))
    status = input("Ingresa el estado del reto: ").strip()
    get_challenges_by_user_status(session, user_id, status)


def req8(session):
    challenge_id = int(input("Ingresa el challenge_id: "))
    get_leaderboard_by_challenge(session, challenge_id)

