import adbc_driver_snowflake.dbapi
import duckdb
import dotenv
import os
# from read_private_key import read_private_key # Uncomment if you're using a private key

# Load environment variables
dotenv.load_dotenv()

# Configuration for Snowflake connection
# pem_key = read_private_key(os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH'), os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'))

SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'role': os.getenv('SNOWFLAKE_ROLE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'), # Comment this line if you want to use a password
    # 'adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value': pem_key, # Comment this line if you want to use a password
    # 'adbc.snowflake.sql.auth_type': 'auth_jwt' # Comment this line if you want to use a password
}

# Create Snowflake connection
snowflake_conn = adbc_driver_snowflake.dbapi.connect(
    db_kwargs={
        'adbc.snowflake.sql.account': SNOWFLAKE_CONFIG['account'],
        'adbc.snowflake.sql.warehouse': SNOWFLAKE_CONFIG['warehouse'],
        'adbc.snowflake.sql.role': SNOWFLAKE_CONFIG['role'],
        'adbc.snowflake.sql.database': SNOWFLAKE_CONFIG['database'],
        'username': SNOWFLAKE_CONFIG['user'],
        'password': SNOWFLAKE_CONFIG['password'], # Comment this line if you want to use a password
        # 'adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value': pem_key, # Uncomment this line if you want to use a password
        # 'adbc.snowflake.sql.auth_type': 'auth_jwt' # Uncomment this line if you want to use a password
    }
)
print("Connection to Snowflake successful.")
snowflake_cursor = snowflake_conn.cursor()

# Query Snowflake
query = """
    SELECT
        *
    FROM SANDBOX_DB.KYLE_SCHEMA.RAW_ORDERS
"""
snowflake_cursor.execute(query)
print("Query executed successfully.")

# Store results as an arrow table
arrow_table = snowflake_cursor.fetch_arrow_table()

# Create DuckDB connection and store locally to path
duckdb_conn = duckdb.connect('demo.db')

# Store arrow table in DuckDB
table_name = 'raw_orders'
query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM arrow_table
"""
duckdb_conn.execute(query)
print("Table created in DuckDB successfully.")
# Alternatively you can use the from_arrow_table method
# duckdb_conn.from_arrow_table(arrow_table)

# Close connections
snowflake_cursor.close()
snowflake_conn.close()
duckdb_conn.close()

print("All done :)")

# Now we can spin up DuckDB CLI and work with the data locally! Enter the following command in your terminal to connect:
# duckdb demo.db