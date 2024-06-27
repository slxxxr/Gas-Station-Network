import psycopg2
import config

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


def get_refuelings_id():
    with connection.cursor() as cursor, open("refuelings_id.txt","w") as file:
        cursor.execute("SELECT ref_id FROM refueling;")
        ref_id = cursor.fetchall()
        for id in ref_id:
            file.write(str(id[0]) + '\n')

def main():
    connection_to_db()
    get_refuelings_id()
    connection_close()


main()
