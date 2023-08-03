import mysql.connector
import json
import os
import openai

# OpenAI API Key
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Connect to the MySQL server
cnx = mysql.connector.connect(user='root', password='root', host='127.0.0.1', database='classicmodels')

# Create a cursor
cur = cnx.cursor()

# Query to retrieve all tables in the specified database
cur.execute("SHOW TABLES")

# Fetch all tables
tables = cur.fetchall()

# Initialize the schema dictionary
schema = {}

# Loop through each table
for (table_name,) in tables:
    # Query to retrieve column information
    cur.execute(f"SHOW COLUMNS FROM {table_name}")
    
    # Fetch all columns
    columns = cur.fetchall()
    
    # Store the table and column information in the schema dictionary
    schema[table_name] = [{'field': column[0] if isinstance(column[0], str) else column[0].decode(), 
                           'type': column[1] if isinstance(column[1], str) else column[1].decode(), 
                           'null': column[2] if isinstance(column[2], str) else column[2].decode(), 
                           'key': column[3] if isinstance(column[3], str) else column[3].decode(), 
                           'default': column[4] if column[4] is None else (column[4] if isinstance(column[4], str) else column[4].decode()), 
                           'extra': column[5] if isinstance(column[5], str) else column[5].decode()} for column in columns]

# Convert the schema dictionary to a JSON string
schema_json = json.dumps(schema)

print(schema_json)