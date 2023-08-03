import mysql.connector
import json
import openai

openai.api_key = "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"

personal_data_keywords = [
    "name",
    "email",
    "phone",
    "address",
    "ssn",
    "social security",
    "dob",
    "date of birth",
    "credit card",
    "account number",
    "password",
]

def convert_bytes_to_string(obj):
    if isinstance(obj, bytes):
        return obj.decode('utf-8')
    raise TypeError


def get_column_descriptions(table_name, cursor):
    cursor.execute(f"SHOW FULL COLUMNS FROM {table_name}")
    columns = cursor.fetchall()
    column_descriptions = [column[8] for column in columns]

    return column_descriptions


def contains_personal_data(column_data):
    response = openai.Completion.create(
        engine="text-davinci-002",
        prompt=column_data,
        max_tokens=100,
        temperature=0.7,
        n=1
    )

    return "yes" in response["choices"][0]["text"].lower()


def get_database_metadata(host_parameter, user_parameter, password_parameter, database_parameter):
    try:
        connection = mysql.connector.connect(host=host_parameter, user=user_parameter, password=password_parameter,
                                             database=database_parameter)
        cursor = connection.cursor()

        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        metadata = {}

        for table in tables:
            table_name = table[0]
            table_metadata = {}

            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]

            column_descriptions = get_column_descriptions(table_name, cursor)

            column_data = [
                f"{column_name} {table_name} {column_description}"
                for column_name, column_description in zip(column_names, column_descriptions)
            ]

            personal_data_columns = [contains_personal_data(column_data) for column_data in column_data]

            personal_data_columns_names = [column_names[i] for i, is_personal_data in enumerate(personal_data_columns)
                                           if is_personal_data]
            if personal_data_columns_names:
                print(f"Table: {table_name}, Personal Data Columns: {', '.join(personal_data_columns_names)}")

            for column_info, is_personal_data in zip(columns, personal_data_columns):
                column_info_dict = dict(zip(column_names, column_info))
                column_name = column_info_dict['Field']
                column_type = column_info_dict['Type']

                table_metadata[column_name] = column_type

                if is_personal_data:
                    table_metadata[f"{column_name}_is_personal_data"] = True

            metadata[table_name] = table_metadata

        cursor.close()
        connection.close()

        return metadata

    except Exception as e:
        print(f"Error: {str(e)}")
        return None


def convert_metadata_to_json(metadata):
    return json.dumps(metadata, indent=4)


if __name__ == "__main__":
    host = "localhost"
    user = "root"
    password = "root"
    database = "classicmodels"

    metadata = get_database_metadata(host, user, password, database)
    json_string = convert_metadata_to_json(metadata)
    print(json_string)
