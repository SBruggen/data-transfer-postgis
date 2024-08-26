DATA TRANSFER POSTGIS

Work in progress

- Loads in configuration of databases from json file (location set at ../../data) in the following format:
{
    "databases": {
        "database 1": {
			"dbname": dbname,
			"user": user,
			"password": pw,
			"host": host,
			"port": "5432"
        },
        "database 2": {
			"dbname": dbname,
			"user": user,
			"password": pw,
			"host": host,
			"port": "5432"
        }
    }
}

- Checks connection status of databases.
- Allows to add tables to the destination database
- Transfer data from source to destination database (work in progress)
