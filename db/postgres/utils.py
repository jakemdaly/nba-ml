import psycopg2
from configparser import ConfigParser


CONNECTION_INFO = {
    'host': 'localhost',
    'database': 'NBA',
    'user': 'postgres',
    'password': 'postgres'
}

def connect():
    '''
    Establishes a connection to the postgres server
    Returns:
        Postgres Client connection
    '''
    connection = psycopg2.connect( f"user={CONNECTION_INFO['user']} password={CONNECTION_INFO['password']}" )
    return connection

def create_db(conn:psycopg2.extensions.connection, db_name:str):
    '''
    Creates a database called db_name using the provided connection 
    '''
    cur = conn.cursor()
    conn.autocommit = True # required for creating database
    sql_query = f"CREATE DATABASE {db_name}"

    try:
        cur.execute(sql_query)
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        print(f"Query: {cur.query}")
        cur.close()
    else:
        # Revert autocommit settings
        conn.autocommit = False


def create_table( sql_query: str, conn: psycopg2.extensions.connection) -> None:
    '''
    Use this function to create a table using a SQL query, note that this does not help
    construct the query, it only helps issue and commit the SQL. 
    Args:
        sql_query : the entire query that you wish to perform
        conn      : the connection to the pg client 
    '''
    try:
        cur = conn.cursor()
        # Execute the table creation query
        cur.execute(sql_query)

    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        print(f"Query: {cur.query}")
        conn.rollback()
        cur.close()

    else:
        # To take effect, changes need be committed to the database
        conn.commit()