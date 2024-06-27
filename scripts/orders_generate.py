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


def insert_orders():
    def normalize(element, used_elements):
        while element in used_elements:
            element = np.random.randint(low=10_000_000, high=99_000_000)

        used_elements.add(element)
        return element

    used_id = set()
    start_time = datetime(2020, 1, 1, 0, 0, 0)
    end_time = datetime(2024, 1, 1, 0, 0, 0)

    with connection.cursor() as cursor, open("employee_id.txt", "r") as file:
        for line in file:
            emp_id, ref_id = map(int,line.split())
            max_cnt = np.random.randint(low=30, high=130)
            for i in range(max_cnt):
                ord_id = np.random.randint(low=10_000_000, high=99_000_000)
                ord_id = normalize(ord_id, used_id)

                cursor.execute("select cl_id from client TABLESAMPLE SYSTEM_ROWS(1);")
                cl_id = cursor.fetchone()[0] if (i % 123 != 0) else 'NULL'

                cursor.execute(f"select ref_srvs from refueling where ref_id = {ref_id};")
                srvs_count = cursor.fetchone()[0] * np.random.randint(low=0, high=5)

                fuel_id = np.random.randint(low=1, high=5 + 1)
                fuel_count = np.random.randint(low=1, high=15 + 1)

                ord_time = fake.date_time_between(start_date=start_time, end_date=end_time)
                ord_paym = random.choice(['cash', 'card'])

                cursor.execute(f"INSERT INTO orders "
                               f"(ord_id, cl_id, ref_id, emp_id, fuel_id, "
                               f"fuel_count, srvs_count, ord_time, ord_paym ) VALUES ( "
                               f"{ord_id}, {cl_id}, {ref_id}, {emp_id}, {fuel_id},"
                               f"{fuel_count},{srvs_count},'{ord_time}','{ord_paym}');")


def main():
    connection_to_db()
    insert_orders()
    connection_close()


main()
