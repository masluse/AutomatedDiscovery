import os
import openai
import pymysql
import prettytable
from Openai.V3.extract_metadata import get_metadata, CustomJsonEncoder
import json

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
openai.api_key = OPENAI_API_KEY

host = 'localhost'
user = 'root'
password = 'root'
database = 'sakila'


def chat(system_content, prompt_content):
    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo-16k",
        messages=[
            {
                "role": "system",
                "content": system_content
            },
            {
                "role": "user",
                "content": prompt_content
            }
        ],
        temperature=0,
        max_tokens=600,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )
    return completion["choices"][0]["message"]["content"]


def get_queries(metadata_parameter, relations_parameter, first_name, last_name):
    if metadata_parameter and relations_parameter:
        # Remove indent to reduce size of JSON string
        metadata_json = json.dumps(metadata, cls=CustomJsonEncoder)
        relations_json = json.dumps(relations, cls=CustomJsonEncoder)

        system_template: str = (
            "Your job is to write queries given a user’s request or do Everything the User wants with the following "
            "Data. \n Metadata: [METADATA] \n Relations: [RELATIONS]")

        prompt_template: str = (
            "You were provided with the Relational Databases Metadata and Relations so can you give me a query that "
            "gives me all the information of a person. Placeholder for the Firstname and Lastname in the query should "
            "be [Firstname] and [Lastname] The query should show all the tables that have something to do with the user"
            "and the query should be provided like this 'SELECT * FROM something;' Do each query in one line and no "
            "other text than the query itself.")
        results = []
        system_message = system_template.replace("[METADATA]", metadata_json)
        system_message = system_message.replace("[RELATIONS]", relations_json)

        prompt_message = prompt_template.replace("[Firstname]", first_name)
        prompt_message = prompt_message.replace("[Lastname]", last_name)

        result_text = chat(system_message, prompt_message)
        results.append(result_text)

        final_result = ' '.join(results)
        final_result = format_output(final_result)
        return final_result
    else:
        return None


def format_output(output_text):
    lines = output_text.strip().split('\n')
    formatted_output = []

    for line in lines:
        line = line.strip()
        if line and line not in formatted_output:
            formatted_output.append(line)

    formatted_output_str = '\n'.join(formatted_output)
    return formatted_output_str


def run_query(query):
    connection = pymysql.connect(host=host, user=user, password=password, database=database)
    cursor = connection.cursor()

    try:
        # Execute each SQL query
        for q in query.split(";"):
            if q.strip():  # Skip empty queries
                cursor.execute(q.strip())
                result = cursor.fetchall()

                # Print the result in a table
                if result:
                    table = prettytable.PrettyTable()
                    table.field_names = [desc[0] for desc in cursor.description]
                    for row in result:
                        table.add_row(row)
                    print(table)
                else:
                    print("No results for the query.")
    except Exception as e:
        print(f"Error executing query: {str(e)}")

    # Close the database connection
    cursor.close()
    connection.close()


if __name__ == "__main__":
    first_name = input("Enter firstname: ")
    last_name = input("Enter lastname: ")

    metadata, relations = get_metadata(host, user, password, database)
    sql_queries = get_queries(metadata, relations, first_name, last_name)

    print("Queries: ")
    print(sql_queries)
    print("============================================================================================================")
    print("Tables: ")
    run_query(sql_queries)
