import psycopg2
import config
import matplotlib.pyplot as plt

connection = None
end_line = "-" * 35
type_employee = None


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


def get_fuel_sales():
    start_date = input("Введите дату начала (гггг-мм-дд): ")
    end_date = input("Введите дату конца (гггг-мм-дд): ")

    try:
        with connection.cursor() as cursor:
            query = """
                SELECT 
                    fuel.fuel_type, 
                    SUM(orders.fuel_count) AS total_sold 
                FROM 
                    orders 
                    JOIN fuel ON orders.fuel_id = fuel.fuel_id
                WHERE 
                    orders.ord_time BETWEEN %s AND %s
                GROUP BY 
                    fuel.fuel_type;
                """
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()
            for row in results:
                print(f"Вид топлива: {row[0]}, Продано литров: {row[1]}")
    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")


def add_client():
    cl_id = input("Введите id клиента: ")
    cl_name = input("Введите имя клиента: ")
    cl_discnt = input("Введите размер скидки клиента: ")
    cl_phone = input("Введите номер телефона клиента: ")
    cl_email = input("Введите адрес электронной почты клиента: ")

    try:
        with connection.cursor() as cursor:
            query = """
            INSERT INTO client (cl_id, cl_name, cl_discnt, cl_phone, cl_email)
            VALUES (%s, %s, %s, %s, %s);
            """
            cursor.execute(query, (cl_id, cl_name, cl_discnt, cl_phone, cl_email))
            connection.commit()
            print("Клиент успешно добавлен.")
    except Exception as e:
        print(f"Ошибка при добавлении клиента: {e}")


def delete_client():
    cl_id = input("Введите id клиента: ")
    try:
        with connection.cursor() as cursor:
            query = "DELETE FROM client WHERE cl_id = %s;"
            cursor.execute(query, (cl_id,))
            connection.commit()
            print("Клиент успешно удален.")
    except Exception as e:
        print(f"Ошибка при удалении клиента: {e}")


def update_client():
    cl_id = input("Введите id клиента: ")
    cl_name = input("Введите имя клиента или -1, если не нужно менять: ")
    cl_discnt = input("Введите размер скидки клиента или -1, если не нужно менять: ")
    cl_phone = input("Введите номер телефона клиента или -1, если не нужно менять: ")
    cl_email = input("Введите адрес электронной почты клиента или -1, если не нужно менять: ")

    try:
        with connection.cursor() as cursor:
            query = "UPDATE client SET "
            updates = []
            params = []
            if cl_name != "-1":
                updates.append("cl_name = %s")
                params.append(cl_name)
            if cl_discnt != "-1":
                updates.append("cl_discnt = %s")
                params.append(cl_discnt)
            if cl_phone != "-1":
                updates.append("cl_phone = %s")
                params.append(cl_phone)
            if cl_email != "-1":
                updates.append("cl_email = %s")
                params.append(cl_email)
            query += ", ".join(updates)
            query += " WHERE cl_id = %s;"
            params.append(cl_id)
            cursor.execute(query, tuple(params))
            connection.commit()
            print("Данные клиента успешно обновлены.")
    except Exception as e:
        print(f"Ошибка при обновлении данных клиента: {e}")


def count_clients():
    try:
        with connection.cursor() as cursor:
            query = "SELECT COUNT(*) FROM client;"
            cursor.execute(query)
            count = cursor.fetchone()[0]
            print(f"Количество записей в таблице client: {count}")
            return count
    except Exception as e:
        print(f"Ошибка при подсчете записей: {e}")
        return None


def add_employee():
    emp_id = input("Введите id сотрудника: ")
    emp_name = input("Введите имя сотрудника: ")
    ref_id = input("Введите id автозаправки: ")
    emp_post = input("Введите должность сотрудника: ")
    emp_phone = input("Введите номер телефона сотрудника: ")

    try:
        with connection.cursor() as cursor:
            query = """
            INSERT INTO employee (emp_id, emp_name, ref_id, emp_post, emp_phone)
            VALUES (%s, %s, %s, %s, %s);
            """
            cursor.execute(query, (emp_id, emp_name, ref_id, emp_post, emp_phone))
            connection.commit()
            print("Сотрудник успешно добавлен.")
    except Exception as e:
        print(f"Ошибка при добавлении сотрудника: {e}")


def delete_employee():
    emp_id = input("Введите id сотрудника: ")
    try:
        with connection.cursor() as cursor:
            query = "DELETE FROM employee WHERE emp_id = %s;"
            cursor.execute(query, (emp_id,))
            connection.commit()
            print("Сотрудник успешно удален.")
    except Exception as e:
        print(f"Ошибка при удалении сотрудника: {e}")


def count_employees_by_city():
    try:
        with connection.cursor() as cursor:
            query = """
            SELECT ref.ref_city, COUNT(emp.emp_id) as employee_count
            FROM refueling ref
            JOIN employee emp ON ref.ref_id = emp.ref_id
            GROUP BY ref.ref_city;
            """
            cursor.execute(query)
            results = cursor.fetchall()
            for row in results:
                print(f"Город: {row[0]}, Количество сотрудников: {row[1]}")

            cities = [row[0] for row in results]
            employee_counts = [row[1] for row in results]

            plt.figure(figsize=(10, 5))
            plt.bar(cities, employee_counts, color='blue')
            plt.xlabel('Город')
            plt.ylabel('Количество сотрудников')
            plt.title('Количество сотрудников в каждом городе')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()

            return results
    except Exception as e:
        print(f"Ошибка при подсчете сотрудников: {e}")
        return None


def add_delivery():
    deli_id = input("Введите id поставки: ")
    ref_id = input("Введите id автозаправки: ")
    fuel_id = input("Введите id топлива: ")
    deli_date = input("Введите дату поставки: ")
    deli_count = input("Введите количество поставки: ")
    try:
        with connection.cursor() as cursor:
            query = """
            INSERT INTO delivery (deli_id, ref_id, fuel_id, deli_date, deli_count)
            VALUES (%s, %s, %s, %s, %s);
            """
            cursor.execute(query, (deli_id, ref_id, fuel_id, deli_date, deli_count))
            connection.commit()
            print("Поставка успешно добавлена.")
    except Exception as e:
        print(f"Ошибка при добавлении поставки: {e}")


def delete_delivery(deli_id):
    deli_id = input("Введите id поставки: ")

    try:
        with connection.cursor() as cursor:
            query = "DELETE FROM delivery WHERE deli_id = %s;"
            cursor.execute(query, (deli_id,))
            connection.commit()
            print("Поставка успешно удалена.")
    except Exception as e:
        print(f"Ошибка при удалении поставки: {e}")


def update_delivery():
    deli_id = input("Введите id поставки: ")
    ref_id = input("Введите id автозаправки или -1, если не нужно изменять: ")
    fuel_id = input("Введите id топлива или -1, если не нужно изменять: ")
    deli_date = input("Введите дату поставки или -1, если не нужно изменять: ")
    deli_count = input("Введите количество поставки или -1, если не нужно изменять: ")
    try:
        with connection.cursor() as cursor:
            query = "UPDATE delivery SET "
            updates = []
            params = []
            if ref_id != "-1":
                updates.append("ref_id = %s")
                params.append(ref_id)
            if fuel_id != "-1":
                updates.append("fuel_id = %s")
                params.append(fuel_id)
            if deli_date != "-1":
                updates.append("deli_date = %s")
                params.append(deli_date)
            if deli_count != "-1":
                updates.append("deli_count = %s")
                params.append(deli_count)
            query += ", ".join(updates)
            query += " WHERE deli_id = %s;"
            params.append(deli_id)
            cursor.execute(query, tuple(params))
            connection.commit()
            print("Поставка успешно обновлена.")
    except Exception as e:
        print(f"Ошибка при обновлении поставки: {e}")


def count_deliveries_by_fuel():
    start_date = input("Введите дату начала (гггг-мм-дд): ")
    end_date = input("Введите дату конца (гггг-мм-дд): ")

    try:
        with connection.cursor() as cursor:
            query = """
            SELECT f.fuel_type, COUNT(d.deli_id) as delivery_count
            FROM delivery d
            JOIN fuel f ON d.fuel_id = f.fuel_id
            WHERE d.deli_date BETWEEN %s AND %s
            GROUP BY f.fuel_type;
            """
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()

            # Печать результатов
            for row in results:
                print(f"Тип топлива: {row[0]}, Количество поставок: {row[1]}")

    except Exception as e:
        print(f"Ошибка при подсчете поставок: {e}")


def add_order():
    ord_id = input("Введите id заказа: ")
    cl_id = input("Введите id клиента или -1, если его нет в базе: ")
    ref_id = input("Введите id автозаправки: ")
    emp_id = input("Введите id сотрудника: ")
    fuel_id = input("Введите id топлива: ")
    srvs_count = input("Введите кол-во доп.услуг: ")
    fuel_count = input("Введите кол-во топлива: ")
    ord_time = input("Введите время заказа в формате (гггг-мм-дд чч:мм:сс): ")
    ord_paym = input("Введите тип оплаты: ")

    if cl_id == "-1":
        cl_id = None

    try:
        with connection.cursor() as cursor:
            query = """
            INSERT INTO orders (ord_id, cl_id, ref_id, emp_id, fuel_id, srvs_count, fuel_count, ord_time, ord_paym)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(query, (ord_id, cl_id, ref_id, emp_id, fuel_id, srvs_count, fuel_count, ord_time, ord_paym))
            connection.commit()
            print("Заказ успешно добавлен.")
    except Exception as e:
        print(f"Ошибка при добавлении заказа: {e}")


def delete_order():
    ord_id = input("Введите id заказа: ")

    try:
        with connection.cursor() as cursor:
            query = "DELETE FROM orders WHERE ord_id = %s;"
            cursor.execute(query, (ord_id,))
            connection.commit()
            print("Заказ успешно удален.")
    except Exception as e:
        print(f"Ошибка при удалении заказа: {e}")


def update_order():
    ord_id = input("Введите id заказа: ")
    cl_id = input("Введите id клиента или -1, если не нужно изменять: ")
    ref_id = input("Введите id автозаправки или -1, если не нужно изменять: ")
    emp_id = input("Введите id сотрудника или -1, если не нужно изменять: ")
    fuel_id = input("Введите id топлива или -1, если не нужно изменять: ")
    srvs_count = input("Введите кол-во доп.услуг или -1, если не нужно изменять: ")
    fuel_count = input("Введите кол-во топлива или -1, если не нужно изменять: ")
    ord_time = input("Введите время заказа в формате (гггг-мм-дд чч:мм:сс) или -1, если не нужно изменять: ")
    ord_paym = input("Введите тип оплаты или -1, если не нужно изменять: ")
    try:
        with connection.cursor() as cursor:
            query = "UPDATE orders SET "
            updates = []
            params = []
            if cl_id != "-1":
                updates.append("cl_id = %s")
                params.append(cl_id)
            if ref_id != "-1":
                updates.append("ref_id = %s")
                params.append(ref_id)
            if emp_id != "-1":
                updates.append("emp_id = %s")
                params.append(emp_id)
            if fuel_id != "-1":
                updates.append("fuel_id = %s")
                params.append(fuel_id)
            if srvs_count != "-1":
                updates.append("srvs_count = %s")
                params.append(srvs_count)
            if fuel_count != "-1":
                updates.append("fuel_count = %s")
                params.append(fuel_count)
            if ord_time != "-1":
                updates.append("ord_time = %s")
                params.append(ord_time)
            if ord_paym != "-1":
                updates.append("ord_paym = %s")
                params.append(ord_paym)
            query += ", ".join(updates)
            query += " WHERE ord_id = %s;"
            params.append(ord_id)
            cursor.execute(query, tuple(params))
            connection.commit()
            print("Заказ успешно обновлен.")
    except Exception as e:
        print(f"Ошибка при обновлении заказа: {e}")


def total_sold_fuel_by_month():
    start_date = input("Введите дату начала (гггг-мм-дд): ")
    end_date = input("Введите дату конца (гггг-мм-дд): ")
    try:
        with connection.cursor() as cursor:
            query = """
            SELECT DATE_TRUNC('month', o.ord_time) as month, SUM(o.fuel_count * f.fuel_price) as total_sold
            FROM orders o
            JOIN fuel f ON o.fuel_id = f.fuel_id
            WHERE o.ord_time BETWEEN %s AND %s
            GROUP BY month
            ORDER BY month;
            """
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()
            for i in range(len(results)):
                results[i] = list(results[i])
                results[i][1] = float(results[i][1].replace('\xa0', '').replace('?', '').replace(',', '.'))

            # Печать результатов
            for row in results:
                print(f"Месяц: {row[0].strftime('%Y-%m')}, Сумма проданного топлива: {row[1]}")

            # Построение столбчатой диаграммы
            results.sort(key=lambda x: x[0])
            months = [row[0].strftime('%Y-%m') for row in results]
            totals = [row[1] for row in results]
            plt.figure(figsize=(10, 5))
            plt.bar(months, totals, color='blue')
            plt.xlabel('Месяц')
            plt.ylabel('Сумма проданного топлива')
            plt.title('Сумма проданного топлива по месяцам')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()

            return results
    except Exception as e:
        print(f"Ошибка при подсчете суммы проданного топлива по месяцам: {e}")
        return None


def total_sold_fuel_by_city():
    start_date = input("Введите дату начала (гггг-мм-дд): ")
    end_date = input("Введите дату конца (гггг-мм-дд): ")
    try:
        with connection.cursor() as cursor:
            query = """
            SELECT r.ref_city, SUM(o.fuel_count * f.fuel_price) as total_sold
            FROM orders o
            JOIN fuel f ON o.fuel_id = f.fuel_id
            JOIN refueling r ON o.ref_id = r.ref_id
            WHERE o.ord_time BETWEEN %s AND %s
            GROUP BY r.ref_city
            ORDER BY r.ref_city;
            """
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()

            for i in range(len(results)):
                results[i] = list(results[i])
                results[i][1] = float(results[i][1].replace('\xa0', '').replace('?', '').replace(',', '.'))

            # Печать результатов
            for row in results:
                print(f"Город: {row[0]}, Сумма проданного топлива: {row[1]}")

            # Построение столбчатой диаграммы
            results.sort(key=lambda x: x[0])
            cities = [row[0] for row in results]
            totals = [row[1] for row in results]
            plt.figure(figsize=(10, 5))
            plt.bar(cities, totals, color='green')
            plt.xlabel('Город')
            plt.ylabel('Сумма проданного топлива')
            plt.title('Сумма проданного топлива по городам')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()

            return results
    except Exception as e:
        print(f"Ошибка при подсчете суммы проданного топлива по городам: {e}")
        return None


def query(type_employee):
    def print_client_command():
        print("Доступные команды:")
        if type_employee == "1":
            print("1. Добавить данные о клиенте")
            print("2. Удалить данные о клиенте")
            print("3. Изменить данные о клиенте")
            print("4. Узнать количество клиентов")
        else:
            print("1. Узнать количество клиентов")
        print(end_line)

    def print_employee_command():
        print("Доступные команды:")
        if type_employee == "1":
            print("1. Добавить данные о сотруднике")
            print("2. Удалить данные о сотруднике")
            print("3. Узнать кол-во сотрудников в каждом городе")
        else:
            print("1. Узнать кол-во сотрудников в каждом городе")
        print(end_line)

    def print_delivery_command():
        print("Доступные команды:")
        if type_employee == "1":
            print("1. Добавить данные о поставке")
            print("2. Удалить данные о поставке")
            print("3. Изменить данные о поставке")
            print("4. Узнать кол-во поставок по каждому виду топлива за определенный период")
        else:
            print("1. Узнать кол-во поставок по каждому виду топлива за определенный период")
        print(end_line)

    def print_orders_command():
        print("Доступные команды:")
        if type_employee == "1":
            print("1. Добавить данные о продаже")
            print("2. Удалить данные о продаже")
            print("3. Изменить данные о продаже")
            print("4. Кол-во продаж каждого вида топлива за определенный период")
            print("5. Выручка с продаж в каждом месяце за определенный период")
            print("6. Выручка с продаж в каждом городе за определенный период")
        else:
            print("1. Кол-во продаж каждого вида топлива за определенный период")
            print("2. Выручка с продаж в каждом месяце за определенный период")
            print("3. Выручка с продаж в каждом городе за определенный период")
        print(end_line)

    def print_command_type():
        print("Доступные категории команд: ")
        print("1. Клиенты")
        print("2. Сотрудники")
        print("3. Поставки топлива")
        print("4. Продажи топлива")
        print("5. Завершить программу")

    print_command_type()
    command_type = input("Введите номер категории: ")
    print(end_line)
    if command_type == "1":
        print_client_command()
        input_command = input("Введите номер команды: ")
        if type_employee == "1":
            if input_command == "1":
                add_client()
            elif input_command == "2":
                delete_client()
            elif input_command == "3":
                update_client()
            elif input_command == "4":
                count_clients()
            else:
                print("Неверный ввод")
        else:
            if input_command == "1":
                count_clients()
            else:
                print("Неверный ввод")

    elif command_type == "2":
        print_employee_command()
        input_command = input("Введите номер команды: ")
        if type_employee == "1":
            if input_command == "1":
                add_employee()
            elif input_command == "2":
                delete_employee()
            elif input_command == "3":
                count_employees_by_city()
            else:
                print("Неверный ввод")
        else:
            if input_command == "1":
                count_employees_by_city()
            else:
                print("Неверный ввод")
    elif command_type == "3":
        print_delivery_command()
        input_command = input("Введите номер команды: ")
        if type_employee == "1":
            if input_command == "1":
                add_delivery()
            elif input_command == "2":
                delete_delivery()
            elif input_command == "3":
                update_delivery()
            elif input_command == "4":
                count_deliveries_by_fuel()
            else:
                print("Неверный ввод")
        else:
            if input_command == "1":
                count_deliveries_by_fuel()
            else:
                print("Неверный ввод")

    elif command_type == "4":
        print_orders_command()
        input_command = input("Введите номер команды: ")
        if type_employee == "1":
            if input_command == "1":
                add_order()
            elif input_command == "2":
                delete_order()
            elif input_command == "3":
                update_order()
            elif input_command == "4":
                get_fuel_sales()
            elif input_command == "5":
                total_sold_fuel_by_month()
            elif input_command == "6":
                total_sold_fuel_by_city()
            else:
                print("Неверный ввод")
        else:
            if input_command == "1":
                get_fuel_sales()
            elif input_command == "2":
                total_sold_fuel_by_month()
            elif input_command == "3":
                total_sold_fuel_by_city()
            else:
                print("Неверный ввод")
    elif command_type == "5":
        return "Exit"
    else:
        print("Неверный ввод")
        return
    print(end_line)


def main():
    connection_to_db()

    def auth_employee():
        print(end_line)
        print("Доступные режима входа: ")
        print("1. Администратор", "2. Бухгалтер", sep='\n')
        input_type_employee = input("Введите номер режима входа: ")
        global type_employee
        if input_type_employee == "1" or input_type_employee == "2":
            type_employee = input_type_employee
            print("Режим входа:", "Администратор" if type_employee == "1" else "Бухгалтер")
        else:
            print("Введен неверный номер")
            return
        print(end_line)

    def start_query():
        while True:
            tmp_value = query(type_employee)
            if tmp_value == "Exit":
                break

    auth_employee()
    start_query()

    connection_close()


main()
