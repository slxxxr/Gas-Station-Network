import psycopg2
import numpy as np
import random
import config
from faker import Faker

fake = Faker('en_CA')
connection = None

def read_city():
    all_city = []
    with open("city.txt", "r") as file:
        for name in file:
            all_city.append(name.rstrip())

    return all_city


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


def insert_refueling():
    used_address = set()
    used_id = set()

    all_city = read_city()

    def normalize(element, used_elements, elem_type):
        if elem_type == "id":
            while element in used_elements:
                element = np.random.randint(low=10_000, high=30_000)
        else:
            while element in used_elements:
                element = fake.street_address()

        used_elements.add(element)
        return element

    with connection.cursor() as cursor:
        for i in range(100):
            ref_id = np.random.randint(low=10_000, high=30_000)
            ref_city = random.choice(all_city)
            ref_address = fake.street_address()
            ref_srvs = random.choice([0,1])

            ref_id = normalize(ref_id,used_id,"id")
            ref_address = normalize(ref_address, used_address, "address")

            cursor.execute(f"INSERT INTO refueling "
                       f"(ref_id, ref_city, ref_address, ref_srvs) VALUES "
                       f"({ref_id}, '{ref_city}', '{ref_address}', '{ref_srvs}');")


def main():
    connection_to_db()
    insert_refueling()
    connection_close()

main()
