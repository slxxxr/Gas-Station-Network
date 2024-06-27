import psycopg2
import numpy as np
import random
import config
from faker import Faker
from datetime import datetime

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


def insert_delivery():

    date_start = datetime(2020, 1, 1)
    date_end = datetime(2024, 1, 1)

    def normalize(element,used_element):
        while element in used_element:
            element = deli_id = np.random.randint(low=10_000_000, high=99_000_000)

        return element

    used_id = set()

    with connection.cursor() as cursor, open('refuelings_id.txt', 'r') as file:
        for ref_id in file:
            max_cnt = np.random.randint(low=5, high=20)
            for i in range(max_cnt):
                deli_id = np.random.randint(low=10_000_000, high=99_000_000)
                fuel_id = np.random.randint(low=1, high=5+1)
                deli_date = fake.date_between_dates(date_start=date_start, date_end=date_end)
                deli_count = np.random.randint(low=100, high=500)

                deli_id = normalize(deli_id,used_id)

                cursor.execute(f"INSERT INTO delivery "
                      f"(deli_id, ref_id, fuel_id, deli_date, deli_count) VALUES "
                      f"({deli_id},{ref_id},{fuel_id},'{deli_date}',{deli_count});")


def main():
    connection_to_db()
    insert_delivery()
    connection_close()

main()
