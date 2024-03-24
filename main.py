import json
from pymongo import MongoClient


class DatabaseManager:
    def __init__(self, db_url):
        self.client = MongoClient(db_url)
        self.db = self.client.get_database()

    def add_data(self, table_name, name=None, surname=None, age=None, student_id=None, advisor_id=None):
        collection = self.db[table_name]
        if table_name != "student_advisor":
            data = {"name": name, "surname": surname, "age": age}
        else:
            data = {"student_id": student_id, "advisor_id": advisor_id}
        collection.insert_one(data)

    def get_existing_relations(self):
        collection = self.db["student_advisor"]
        existing_relations = collection.find({}, {"student_id": 1, "advisor_id": 1, "_id": 0})
        return existing_relations

    def delete_row(self, table_name, row_id):
        collection = self.db[table_name]
        if table_name == "advisors":
            collection.delete_one({"advisor_id": row_id})
        else:
            collection.delete_one({"student_id": row_id})

    def load_data(self, table_name):
        collection = self.db[table_name]
        return list(collection.find())

    def search(self, table_name, name=None, surname=None, age=None, student_id=None, advisor_id=None):
        collection = self.db[table_name]
        query = {}
        if student_id:
            query["student_id"] = {"$regex": student_id}
        if advisor_id:
            query["advisor_id"] = {"$regex": advisor_id}
        if name:
            query["name"] = {"$regex": name}
        if surname:
            query["surname"] = {"$regex": surname}
        if age:
            query["age"] = {"$regex": age}
        if query:
            return list(collection.find(query))
        else:
            return self.load_data(table_name)

    def update(self, table_name, name, surname, age, id_in):
        collection = self.db[table_name]
        if table_name == "students":
            collection.update_one({"student_id": id_in}, {"$set": {"name": name, "surname": surname, "age": age}})
        elif table_name == "advisors":
            collection.update_one({"advisor_id": id_in}, {"$set": {"name": name, "surname": surname, "age": age}})

    def check_bd(self):
        collection = self.db["student_advisor"]
        return not collection.find_one()

    def list_advisors_with_students_count(self, order_by):
        pipeline = [
            {"$lookup": {"from": "student_advisor", "localField": "advisor_id",
                         "foreignField": "advisor_id", "as": "students"}},
            {"$project": {"advisor_id": 1, "name": 1, "surname": 1, "student_count": {"$size": "$students"}}},
            {"$sort": {"student_count": order_by}}
        ]
        return list(self.db["advisors"].aggregate(pipeline))

    def list_students_with_advisors_count(self, order_by):
        pipeline = [
            {"$lookup": {"from": "student_advisor", "localField": "student_id",
                         "foreignField": "student_id", "as": "advisors"}},
            {"$project": {"student_id": 1, "name": 1, "surname": 1, "advisor_count": {"$size": "$advisors"}}},
            {"$sort": {"advisor_count": order_by}}
        ]
        return list(self.db["students"].aggregate(pipeline))


def regions():
    with open("Data\\data.json", "r") as f:
        data = json.load(f)
    return list(data.keys())


db_manager = DatabaseManager("mongodb://localhost:27017/test_database")
