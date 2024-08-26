import json
import os
from db_connection import connect_to_database
from db_manipulations import fetch_data_from_source, insert_data_into_destination, create_table_if_not_exists, get_user_input_for_columns, create_table_from_json

def main():
    # Load the JSON configuration file
    loc_config = os.path.join('..', '..', 'data', 'configdb.json')
    with open(loc_config, 'r') as f:
        config = json.load(f)

    print("Configuration file loaded with the following databases:")
    for key in config['databases']:
        print (f"- {key}")

    #print(config['databases'])
    # select and connect to databases
    db_source = input('Select the source database (default: geoit) :') or "geoit"
    db_dest = input('Select the destination database (default: aarschot geoloket) :') or "aarschot"

    db_params_source = config['databases'][db_source]
    db_params_dest = config['databases'][db_dest]

    # Establish a database connection
    source_conn = connect_to_database(db_params_source['host'], db_params_source['dbname'], db_params_source['user'], db_params_source['password'])
    dest_conn = connect_to_database(db_params_dest['host'], db_params_dest['dbname'], db_params_dest['user'], db_params_dest['password'])

    if source_conn and dest_conn:

        # Create cursors using the connection objects
        source_cursor = source_conn.cursor()
        dest_cursor = dest_conn.cursor()

        # first step: adding tables to source database (multiple tables can be added)
        while True:
            add_table = input("Do you want to add a table in the source database? (yes/no): ")
            if add_table not in ['yes', 'y', 'ok']:
                print("No new table will be added to the database.")
                break

            # Choose method of loading table structure
            table_input = int(input("Enter the table structure from a json file (1) or from manual input(2): "))

            # Give schema and table name for storing in database
            schema = input("Enter the schema where the table will be created (default 'public'): ") or "public"
            table_name = input("Enter the name of the table to create: ")

            # Create table based on JSON definition
            if table_input == 1:
                json_map = input(r"Enter the path to the JSON file with the table definition (map structure, default from desktop\work code\data\tables): ") or r"C:\Users\stijn.bruggen\Desktop\work code\data\tables"
                json_file = input("Enter the name of the json file with the table definition (.json): ")
                json_path = os.path.join(json_map, json_file)
                print(f"Attempting to create table from location {json_path}")
                columns_dict = create_table_from_json(dest_cursor, json_path)

            # Create table by manual input
            elif table_input == 2:
                columns_dict = get_user_input_for_columns()
        
            # Create the table
            create_table_if_not_exists(dest_cursor, schema, table_name, columns_dict)

            dest_conn.commit()
        
        # Close the cursors after completing the operations
        source_cursor.close()
        dest_cursor.close()

        '''columns = ['id', 'Naam', 'Adres', 'Opmerkingen', 'geometry']
        table_name = 'editeren.elementen_aed_24001'
        data = fetch_data_from_source(source_conn, table_name, columns)'''
        

    else:
        print("Failed to connect to one or both databases.")

if __name__ == "__main__":
    main()