import psycopg2
from time import sleep

def connect_to_database(host, database, user, password, attempts=3, delay=5):
    while attempts > 0:
        try:
            conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )
            print(f"Database connection established to database {database}.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Failed to connect to the database: {e}")
            attempts -= 1
            print(f"Retrying... {attempts} attempts left.")
            sleep(delay)
    print("Connection failed after multiple attempts.")
    return None
