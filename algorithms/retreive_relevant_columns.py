import json
import openai
import os
from extract_metadata import get_metadata, CustomJsonEncoder


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


def get_relevant_columns(metadata_parameter, relations_parameter):
    if metadata_parameter and relations_parameter:
        metadata_json: str = json.dumps(metadata_parameter, cls=CustomJsonEncoder, indent=2)
        relations_json: str = json.dumps(relations_parameter, cls=CustomJsonEncoder, indent=2)

        system_template: str = (
            "These are the tables metadata:\n[METADATA]\nand these are the relations of the table:\n"
            "[RELATIONS].\nReturn a non numbered list of all columns that "
            "contain personal data. Do it in this format: '- customer: first_name, last_name, email, address_id' and "
            "no spaces around it")

        prompt_template: str = (
            "Search all the data and return a non numbered list of all columns that "
            "contain personal data. Do it in this format: '- customer: first_name, last_name, email, address_id' and "
            "no spaces around it")

        results = []
        system_message = system_template.replace("[METADATA]", metadata_json)
        system_message = system_message.replace("[RELATIONS]", relations_json)

        result_text = chat(system_message, prompt_template)
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


if __name__ == "__main__":
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
    openai.api_key = OPENAI_API_KEY

    host = 'localhost'
    user = 'root'
    password = 'root'
    database = 'sakila'

    metadata, relations = get_metadata(host, user, password, database)
    relevant_columns = get_relevant_columns(metadata, relations)
    print(relevant_columns)
