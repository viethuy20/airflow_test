import mysql.connector as connector
import pandas as pd
# DB_CONFIG = {
#     'host': 'localhost',  # Use 'localhost' for local MySQL server
#     'port': 3306, 
#     'user': 'root',
#     'password': 'root', 
#     'database': 'sakila',
#     'raise_on_warnings': True,  
# }
DB_CONFIG = {
    'host': 'host.docker.internal',
    'port': 3306, 
    'user': 'root',
    'password': 'root', 
    'database': 'sakila',
    'raise_on_warnings': True,  
}
def connect_to_db():
    try:
        con = connector.connect(**DB_CONFIG)
        if con.is_connected():
            print("Connected to the database")
            return con
    except connector.Error as err:
        print(f"Error: {err}")
        return None
    
def query_db(query):
    con = connect_to_db()
    if con:
        cursor = con.cursor()
        try:
            cursor.execute(query)
            results = cursor.fetchall() 
            col = [i[0] for i in cursor.description]

        except connector.Error as err:
            results = []
            col = []
            print(f"Query Error: {err}")
        finally:
            cursor.close()
            con.close()
        return results, col
    else:
        print("Failed to connect to the database for querying.")
        return [], []

# if __name__ == "__main__":
#     query = "SELECT * FROM actor LIMIT 5;"
#     query_db(query)
