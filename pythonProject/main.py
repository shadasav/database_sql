import psycopg2
from config import host, user, db_name, password
#connection = None
def create_connection():
    try:
        # подключение к базе
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name
        )
        connection.autocommit = True
        return connection
    except Exception as _ex:
        print("[INFO] Error while working with PostgreSQL", _ex)
        return None

def fetch_data(query):
    connection = create_connection()
    if connection is None:
        return None

    try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                return result
    except Exception as _ex:
        print("[INFO] Error while fetching data", _ex)
        return None
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    query = "SELECT DISTINCT customers.contact_name, customers.address, orders.ship_address FROM customers JOIN orders ON customers.customer_id = orders.customer_id;"
    data = fetch_data(query)
    if data:
        filtered_data = [row for row in data if row[1] != row[2]]
        for row in filtered_data:
            print(row)
