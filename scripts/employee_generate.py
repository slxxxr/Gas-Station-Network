import psycopg2
import numpy as np
import random
import config
from faker import Faker

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


def insert_employee():
    def normalize(element, used_elements, elem_type):
        if (elem_type == "name"):
            while (element in used_elements) or element.count('.') or len(element.split()) != 2:
                element = fake.name()

        if (elem_type == "phone"):
            while element in used_elements:
                element = "+1" + "".join([str(random.randint(0, 9)) for i in range(9)])

        if (elem_type == "id"):
            while element in used_elements:
                element = np.random.randint(low=10_000_000, high=99_000_000)

        used_elements.add(element)
        return element

    used_id = set()
    used_name = set()
    used_phone = set()

    with connection.cursor() as cursor, open('refuelings_id.txt', 'r') as file:
        for ref_id in file:
            for emp_post,cnt in [('washer',5), ('manager',1), ('operator',2), ('salesman',2)]:
                for i in range(cnt):
                    emp_name = fake.name()
                    emp_id = np.random.randint(low=10_000_000, high=99_000_000)
                    emp_phone = "+1" + "".join([str(random.randint(0, 9)) for i in range(9)])

                    emp_name = normalize(emp_name, used_name, "name")
                    emp_id = normalize(emp_id, used_id, "id")
                    emp_phone = normalize(emp_phone, used_phone, "phone")

                    cursor.execute(f"INSERT INTO employee "
                          f"(emp_id, emp_name, ref_id, emp_post, emp_phone) VALUES "
                          f"({emp_id},'{emp_name}',{ref_id},'{emp_post}','{emp_phone}');")



def main():
     connection_to_db()
     insert_employee()
     connection_close()


main()
