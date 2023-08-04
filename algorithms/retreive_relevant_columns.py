import json
import openai
import os
from extract_metadata import get_metadata, CustomJsonEncoder


def get_relevant_columns(metadata_parameter, relations_parameter):
    if metadata_parameter and relations_parameter:
        metadata_json: str = json.dumps(metadata_parameter, cls=CustomJsonEncoder, indent=2)
        relations_json: str = json.dumps(relations_parameter, cls=CustomJsonEncoder, indent=2)

        prompt_template: str = (
            "These are the tables metadata:\n${metadata}\nand these are the relations of the table:\n"
            "${relations}.\nSearch all the data I provided and return a non numbered list of all columns that "
            "contain personal data. Do it in this format: '- customer: first_name, last_name, email, address_id' and "
            "no spaces around it")

        chunk_size = 1000
        metadata_chunks = [metadata_json[i:i + chunk_size] for i in range(0, len(metadata_json), chunk_size)]
        relations_chunks = [relations_json[i:i + chunk_size] for i in range(0, len(relations_json), chunk_size)]

        results = []
        for metadata_chunk, relations_chunk in zip(metadata_chunks, relations_chunks):
            prompt_message = prompt_template.replace("${metadata}", metadata_chunk)
            prompt_message = prompt_message.replace("${relations}", relations_chunk)

            completion = openai.Completion.create(engine="text-davinci-003", prompt=prompt_message, max_tokens=200)
            result_text = completion.choices[0]['text'].strip()
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
