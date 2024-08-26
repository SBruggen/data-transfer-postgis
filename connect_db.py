import os
import json
import psycopg2
from time import sleep

# Load the JSON configuration file
loc_config = os.path.join('..', '..', 'data', 'configdb.json')
with open(loc_config, 'r') as f:
    config = json.load(f)

db_params_db1 = config['databases']['geoit']
db_params_db2 = config['databases']['aarschot']

# Function to connect to database

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


# Usage
source_conn = connect_to_database(db_params_db1['host'], db_params_db1['dbname'], db_params_db1['user'], db_params_db1['password'])
if source_conn is not None:
    cursor = source_conn.cursor()
    source = True
else:
    source = False
    print(f"Cannot connect to database {db_params_db1['dbname']}.")

dest_conn = connect_to_database(db_params_db2['host'], db_params_db2['dbname'], db_params_db2['user'], db_params_db2['password'])
if dest_conn is not None:
    cursor = dest_conn.cursor()
    dest = True
else:
    dest = False
    print(f"Cannot connect to database {db_params_db2['dbname']}.")