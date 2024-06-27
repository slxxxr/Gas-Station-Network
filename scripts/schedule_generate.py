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


def insert_schedule():
    def normalize(hour, minute, second):
        result = ""
        result += "0" * (hour < 10) + str(hour)
        result += ":" + "0" * (minute < 10) + str(minute)
        result += ":" + "0" * (second < 10) + str(second)

        return "'" + result + "'"

    with connection.cursor() as cursos, open("employee_id.txt", "r") as file:
        for line in file:
            emp_id, ref_id = map(int, line.split())
            week_time = []
            for i in range(7):
                hour = np.random.randint(low=0, high=24)
                minute = random.choice([0, 30])
                second = 0
                week_time.append(normalize(hour, minute, second))

            hol_1 = np.random.randint(low=0, high=7)
            hol_2 = np.random.randint(low=0, high=7)
            while hol_1 == hol_2:
                hol_2 = np.random.randint(low=0, high=7)

            week_time[hol_1] = week_time[hol_2] = 'NULL'


            cursos.execute(f"INSERT INTO emp_schedule"
                  f"(emp_id, "
                  f"monday, tuesday,"
                  f"wednesday, thursday,"
                  f"friday, saturday,"
                  f"sunday) VALUES "
                  f"({emp_id},"
                  f"{week_time[0]}, {week_time[1]},"
                  f"{week_time[2]}, {week_time[3]},"
                  f"{week_time[4]}, {week_time[5]},"
                  f"{week_time[6]});")


def main():
    connection_to_db()
    insert_schedule()
    connection_close()


main()
