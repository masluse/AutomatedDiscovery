import os
import openai
from extract_metadata import get_metadata, CustomJsonEncoder
import json

def main():
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
    #prompt = f"I require a comprehensive list of all columns, including their corresponding table names (in the 'table.column' format), that potentially contain personal data. Please utilize the provided metadata, represented as: {metadata_json}. Additionally, consider the following relationship data: {relations_json}. The analysis should be based on both the metadata and relationships to identify potential instances of personal data."

    # Set up the OpenAI API
    openai.api_key = OPENAI_API_KEY

    # Create a list of messages. In this case, there's only one message.
    messages = [
        {
            "role": "system",
            "content": "I require a comprehensive list of all columns, including their corresponding table names (in the 'table.column' format), that potentially contain personal data. The Output should be no text and no - or 1. but only the Attributes below each other. Please utilize the provided metadata, represented as: {metadata_json}. Additionally, consider the following relationship data: {relations_json}. The analysis should be based on both the metadata and relationships to identify potential instances of personal data."
        }
    ]

    # Send a request to the OpenAI API
    response = openai.ChatCompletion.create(
    model="gpt-3.5-turbo-16k",
    messages=messages
    )

    # Print the response from the OpenAI API
    print(response['choices'][0]['message']['content'])

if __name__ == "__main__":
    main()