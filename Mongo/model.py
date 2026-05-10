#!/usr/bin/env python3

import json
import logging
from datetime import datetime
from pymongo import MongoClient

log = logging.getLogger()

def create_database_and_collections(db):
    collections = db.list_collection_names()

    if "habits" not in collections:
        db.create_collection("habits")
    if "users" not in collections:
        db.create_collection("users")
    if "logs" not in collections:
        db.create_collection("logs")
    if "badges" not in collections:
        db.create_collection("badges")
    print("Colecciones creadas exitosamente.")


def create_indexes(db):
    db.habits.create_index([("user_id", 1)])
    db.habits.create_index([("status", 1)])
    db.habits.create_index([("category", 1)])
    db.habits.create_index([("habit_id", 1)])
    db.logs.create_index([("habit_id", 1)])
    db.logs.create_index([("user_id", 1)])
    db.logs.create_index([("log_date", 1)])
    db.logs.create_index([("status", 1)])
    db.users.create_index([("user_id", 1)])
    db.users.create_index([("email", 1)])
    db.badges.create_index([("badge_id", 1)])
    print("Índices creados exitosamente.")


def load_data(db):
    import csv
    import os
    
    BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "Mongo"))
    
    users_data = []
    with open(os.path.join(BASE_PATH, "user.csv"), newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            badges = json.loads(row["badges"]) if row["badges"] else []
            user_doc = {
                "user_id": row["user_id"],
                "name": row["name"],
                "email": row["email"],
                "register_date": datetime.fromisoformat(row["register_date"]),
                "badges": badges
            }
            users_data.append(user_doc)
    
    if users_data:
        db.users.insert_many(users_data)
    
    habits_data = []
    with open(os.path.join(BASE_PATH, "habits.csv"), newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            habit_doc = {
                "habit_id": row["habit_id"],
                "user_id": row["user_id"],
                "name": row["name"],
                "category": row["category"],
                "frequency": row["frequency"],
                "status": row["status"]
            }
            habits_data.append(habit_doc)
    
    if habits_data:
        db.habits.insert_many(habits_data)
    
    logs_data = []
    with open(os.path.join(BASE_PATH, "logs.csv"), newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            log_doc = {
                "user_id": row["user_id"],
                "habit_id": row["habit_id"],
                "log_date": datetime.fromisoformat(row["log_date"]),
                "status": row["status"],
                "observation": row["observation"]
            }
            logs_data.append(log_doc)
    
    if logs_data:
        db.logs.insert_many(logs_data)
    
    badges_data = []
    with open(os.path.join(BASE_PATH, "badges.csv"), newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            badge_doc = {
                "badge_id": row["badge_id"],
                "name": row["name"],
                "category": row["category"],
                "criteria_summary": row["criteria_summary"]
            }
            badges_data.append(badge_doc)
    
    if badges_data:
        db.badges.insert_many(badges_data)
    
    print("Datos cargados exitosamente.")


def req1(db):
    email = input("Email: ")
    
    pipeline = [
        {
            "$lookup": {
                "from": "users",
                "localField": "user_id",
                "foreignField": "user_id",
                "as": "user"
            }
        },
        {
            "$unwind": "$user"
        },
        {
            "$match": {
                "user.email": email,
                "status": "active"
            }
        },
        {
            "$project": {
                "_id": 0,
                "habit_id": 1,
                "name": 1,
                "frequency": 1,
                "status": 1
            }
        }
    ]
    
    result = list(db.habits.aggregate(pipeline))
    print(json.dumps(result, indent=2, default=str))
    return result


def req2(db):
    pipeline = [
        {
            "$group": {
                "_id": "$user_id",
                "habit_count": {"$sum": 1},
                "habit_name": {"$push": "$name"}
            }
        },
        {
            "$match": {
                "habit_count": {"$gt": 5}
            }
        },
        {
            "$lookup": {
                "from": "users",
                "localField": "_id",
                "foreignField": "user_id",
                "as": "user"
            }
        },
        {
            "$unwind": "$user"
        },
        {
            "$project": {
                "_id": 0,
                "email": "$user.email",
                "habit_count": 1,
                "habit_name": 1
            }
        }
    ]
    
    result = list(db.habits.aggregate(pipeline))
    print(json.dumps(result, indent=2, default=str))
    return result


def req3(db):
    pipeline = [
        {
            "$group": {
                "_id": "$user_id",
                "completion_rate": {
                    "$avg": {
                        "$cond": [{"$eq": ["$status", "completed"]}, 1, 0]
                    }
                }
            }
        },
        {
            "$lookup": {
                "from": "users",
                "localField": "_id",
                "foreignField": "user_id",
                "as": "user"
            }
        },
        {
            "$unwind": "$user"
        },
        {
            "$project": {
                "_id": 0,
                "email": "$user.email",
                "completion_rate": 1
            }
        }
    ]
    
    result = list(db.logs.aggregate(pipeline))
    print(json.dumps(result, indent=2, default=str))
    return result


def req4(db):
    pipeline = [
        {
            "$group": {
                "_id": "$name",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        },
        {
            "$project": {
                "_id": 0,
                "name": "$_id",
                "count": 1
            }
        }
    ]
    
    result = list(db.habits.aggregate(pipeline))
    print(json.dumps(result, indent=2, default=str))
    return result


def req5(db):
    pipeline = [
        {
            "$group": {
                "_id": "$user_id",
                "completion_rate": {
                    "$avg": {
                        "$cond": [{"$eq": ["$status", "completed"]}, 1, 0]
                    }
                }
            }
        },
        {
            "$match": {
                "completion_rate": {"$lt": 0.5}
            }
        },
        {
            "$lookup": {
                "from": "users",
                "localField": "_id",
                "foreignField": "user_id",
                "as": "user"
            }
        },
        {
            "$unwind": "$user"
        },
        {
            "$project": {
                "_id": 0,
                "email": "$user.email",
                "completion_rate": 1
            }
        }
    ]
    
    result = list(db.logs.aggregate(pipeline))
    print(json.dumps(result, indent=2, default=str))
    return result


def req6(db):
    pipeline = [
        {
            "$group": {
                "_id": "$category",
                "habit_count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "category": "$_id",
                "habit_count": 1
            }
        }
    ]
    
    result = list(db.habits.aggregate(pipeline))
    print(json.dumps(result, indent=2, default=str))
    return result


def req7(db):
    log_date_str = input("Fecha (YYYY-MM-DD): ")
    log_date = datetime.fromisoformat(log_date_str)
    
    pipeline = [
        {"$match": {"log_date": log_date}},
        {"$lookup": {
            "from": "users",
            "localField": "user_id",
            "foreignField": "user_id",
            "as": "user"
        }},
        {"$unwind": "$user"},
        {"$lookup": {
            "from": "habits",
            "localField": "habit_id",
            "foreignField": "habit_id",
            "as": "habit"
        }},
        {"$unwind": "$habit"},
        {"$project": {
            "_id": 0,
            "email": "$user.email",
            "habit_name": "$habit.name",
            "habit_id": 1,
            "log_date": 1
        }}
    ]
    
    result = list(db.logs.aggregate(pipeline))
    print(json.dumps(result, indent=2, default=str))
    return result


def req8(db):
    pipeline = [
        {
            "$group": {
                "_id": "$status",
                "count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "status": "$_id",
                "count": 1
            }
        }
    ]
    
    result = list(db.logs.aggregate(pipeline))
    print(json.dumps(result, indent=2, default=str))
    return result


def req9(db):
    email = input("Email: ")
    
    pipeline = [
        {
            "$match": {
                "email": email
            }
        },
        {
            "$unwind": "$badges"
        },
        {
            "$lookup": {
                "from": "badges",
                "localField": "badges.badge_id",
                "foreignField": "badge_id",
                "as": "badge_info"
            }
        },
        {
            "$unwind": "$badge_info"
        },
        {
            "$project": {
                "_id": 0,
                "user_id": 1,
                "badge_id": "$badges.badge_id",
                "name": "$badge_info.name",
                "category": "$badge_info.category",
                "earned_at": "$badges.earned_at",
                "criteria_summary": "$badge_info.criteria_summary"
            }
        }
    ]
    
    result = list(db.users.aggregate(pipeline))
    print(json.dumps(result, indent=2, default=str))
    return result


def req10(db):
    pipeline = [
        {
            "$group": {
                "_id": "$name",
                "total": {"$sum": 1},
                "abandoned": {
                    "$sum": {
                        "$cond": [{"$eq": ["$status", "abandoned"]}, 1, 0]
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "habit_name": "$_id",
                "dropoff_count": "$abandoned",
                "dropoff_rate": {
                    "$divide": ["$abandoned", "$total"]
                }
            }
        },
        {
            "$sort": {"dropoff_rate": -1}
        }
    ]
    
    result = list(db.habits.aggregate(pipeline))
    print(json.dumps(result, indent=2, default=str))
    return result


def delete_all_collections(db):
    confirm = input("¿Eliminar todas las colecciones y datos? Escribe 'SI' para confirmar: ")
    if confirm != "SI":
        print("Operación cancelada.")
        return

    collections = db.list_collection_names()
    if not collections:
        print("No hay colecciones para eliminar.")
        return

    for name in collections:
        db.drop_collection(name)
        print(f"Colección '{name}' eliminada.")

    print("Todas las colecciones fueron eliminadas.")

