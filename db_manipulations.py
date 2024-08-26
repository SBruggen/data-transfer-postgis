import psycopg2
import json

### Setup function

def enable_postgis(cursor):
    """Enable PostGIS extension in the database."""
    cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    print("PostGIS extension enabled.")


### Load data and table structures

def create_table_from_json(cursor, json_path):
    """Create a database table from a JSON schema definition.
    
    Args:
        cursor (Cursor): Database cursor.
        json_path (str): Path to the JSON file containing the table schema.
    """
    with open(json_path, 'r') as file:
        schema = json.load(file)
    
    table_name = schema['table_name']
    columns = schema['columns']
    '''    column_defs = [f"{col_name} {col_type}" for col_name, col_type in columns.items()]
    column_defs_str = ", ".join(column_defs)
    sql_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs_str});"
    print("Executing SQL from json:", sql_query)
    cursor.execute(sql_query)
    print(f"Table '{table_name}' created or already exists based on the schema in {json_path}.")'''

    return schema['columns']


def fetch_data_from_source(conn, table_name, columns):
    """
    Fetch data from a specified table and columns.
    
    Args:
    conn : psycopg2 connection object to the source database.
    table_name (str): Name of the table to fetch data from.
    columns (list): List of column names to fetch.
    
    Returns:
    list of tuples: Retrieved data rows.
    """
    cursor = conn.cursor()
    try:
        column_string = ', '.join([str(column) for column in columns])  # Construct column part of the query
        query = f"SELECT {column_string} FROM {table_name}"
        print("Executing SQL:", query)
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
    except Exception as e:
        print(f"An error occurred while fetching data: {e}")
        return []
    finally:
        cursor.close()


### create tables and insert data functions

def insert_data_into_destination(conn, table_name, columns, data):
    """
    Insert data into a specified table in the destination database.
    
    Args:
    conn : psycopg2 connection object to the destination database.
    table_name (str): Name of the table to insert data into.
    columns (list): List of column names corresponding to the data.
    data (list of tuples): Data to be inserted.
    """
    cursor = conn.cursor()
    try:
        column_string = ', '.join([str(column) for column in columns])  # Columns for INSERT INTO
        placeholders = ', '.join(['%s'] * len(columns))  # Placeholder %s for each column
        query = f"INSERT INTO {table_name} ({column_string}) VALUES ({placeholders})"
        print("Executing SQL:", query)
        cursor.executemany(query, data)
        conn.commit()
        print(f"Successfully inserted {len(data)} records into {table_name}.")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred while inserting data: {e}")
    finally:
        cursor.close()

'''def create_table_if_not_exists(cursor):
    """Create the table if it does not exist.
    Not definitive version yet    
        """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.elementen_aed (
            Id INTEGER PRIMARY KEY,
            Naam TEXT,
            Adres TEXT,
            opmerkingen TEXT
            geometry GEOMETRY(MULTIPOINT)
        );
    """)'''

def create_table_if_not_exists(cursor, schema, table_name, columns_dict):
    """Create a table with given specifications if it does not exist.

    Args:
        cursor (Cursor): Database cursor.
        schema (str): Schema of the table.
        table_name (str): Name of the table to create.
        columns_dict (dict): Dictionary with column names as keys and data types as values.
    """

    # Ensure PostGIS is enabled before attempting to create a table with geometry types
    #enable_postgis(cursor)

    full_table_name = f"{schema}.{table_name}"  # Combining schema and table name
    columns = []
    for col_name, col_type in columns_dict.items():
        columns.append(f"{col_name} {col_type}")
    columns_str = ", ".join(columns)
    sql_query = f"CREATE TABLE IF NOT EXISTS {full_table_name} ({columns_str});"
    try:
        print("Executing SQL:", sql_query)
        cursor.execute(sql_query)
        print(f"Table '{full_table_name}' created or already exists.")
    except Exception as e:
        print(f"Failed to create table {full_table_name}: {e}")

def get_user_input_for_columns():
    columns_dict = {}
    type_map = {
        'T': 'TEXT',
        'I': 'INTEGER',
        'G': 'GEOMETRY'
    }
    geom_type_map = {
        'A': 'MULTIPOINT',
        'B': 'MULTILINESTRING',
        'C': 'MULTIPOLYGON'
    } 

    while True:
        add_column = input("Do you want to add a column to the table? (yes/no): ")
        if add_column not in ['yes', 'y', 'ok']:
            print("No more columns to add. Proceeding to the next step.")
            break
        
        column_name = input("Enter column name: ")
        column_type = input("Enter column type (T for TEXT, I for INTEGER, G for GEOMETRY): ").upper()

        if column_type in type_map:
            column_type = type_map[column_type]

        if column_type == 'GEOMETRY':
            geom_type_input = input("Enter the geometry type (A for MULTIPOINT, B for MULTILINESTRING, C for MULTIPOLYGON): ").upper()
            geom_type = geom_type_map.get(geom_type_input, 'MULTIPOINT')  # Default to MULTIPOINT if invalid input
            srid = input("Enter SRID (default 0, type 'default' or specific number): ")
            srid = '0' if srid.lower() == 'default' else srid
            column_type = f"GEOMETRY({geom_type}, {srid})"

        columns_dict[column_name] = column_type
    
    return columns_dict