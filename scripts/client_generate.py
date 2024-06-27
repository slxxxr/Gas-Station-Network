import psycopg2
import numpy as np
import random
import config
from faker import Faker

def read_names():
    all_names = []
    with open("names.txt", "r") as file:
        for name in file:
            all_names.append(name.rstrip())
    return all_names


fake = Faker('en_CA')
connection = None
def connection_to_db():
    global connection
    try:
        connection = psycopg2.connect(
            dbname=config.db_name,
            user=config.user,
            password=config.password,
            host=config.host)

        connection.autocommit = True

        print("[INFO] Connected to DB")

    except Exception as _ex:
        print("[INFO] Can`t establish connection to database", _ex)

def connection_close():
    if connection:
        connection.close()
        print("[INFO] PostgreSQL connection closed")


def insert_client():
    used_id = set()
    used_phone = set()

    def normalize(element, used_elements, elem_type):
        if elem_type == "phone":
            while element in used_elements:
                element = "+1" + "".join([str(random.randint(0, 9)) for i in range(9)])
        else:
            while element in used_elements:
                element = np.random.randint(low=10_000_000, high=99_000_000)

        used_elements.add(element)
        return element

    with connection.cursor() as cursor:
        for cl_name in read_names():
            cl_id = np.random.randint(low=10_000_000, high=99_000_000)
            cl_email = cl_name.replace(' ', '') + "@gmail.com"
            cl_discnt = random.choice([0, 5, 10])
            cl_phone = "+1" + "".join([str(random.randint(0, 9)) for i in range(9)])

            cl_id = normalize(cl_id, used_id, "id")
            cl_phone = normalize(cl_phone, used_phone, "phone")

            cursor.execute(f"INSERT INTO client "
                           f"(cl_id, cl_name, cl_phone, cl_email, cl_discnt) VALUES "
                           f"({cl_id}, '{cl_name}', '{cl_phone}', '{cl_email}', {cl_discnt});")



def main():
    connection_to_db()
    insert_client()
    connection_close()

main()
