# Snowflake to DuckDB

This script connects to Snowflake, queries the data, and stores it in a local DuckDB database.

## How to use

1. Clone the repository
2. Create a virtual environment using `python -m venv venv`
3. Install the dependencies using `pip install -r requirements.txt`
4. Set up your Snowflake and DuckDB credentials in a `.env`  using the `.env.example` file
5. Run the script using `python main.py`