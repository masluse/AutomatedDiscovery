import os
import openai
from extract_metadata import get_metadata, CustomJsonEncoder
import json

# OpenAI API Key
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Database Details
host = 'localhost'
user = 'root'
password = 'root'
database = 'sakila'

# Get Metadata and Relations
metadata, relations = get_metadata(host, user, password, database)

# Remove indent to reduce size of JSON string
metadata_json = json.dumps(metadata, cls=CustomJsonEncoder)
relations_json = json.dumps(relations, cls=CustomJsonEncoder)

# Create a prompt for the OpenAI API
prompt = f"The database schema is: {metadata_json}. The relations are: {relations_json}. Please generate SQL queries to retrieve user-specific data."

# Set up the OpenAI API
openai.api_key = OPENAI_API_KEY

# Send a request to the OpenAI API
response = openai.Completion.create(engine="text-davinci-002", prompt=prompt, max_tokens=200)

# Print the response from the OpenAI API
print(response.choices[0].text.strip())
