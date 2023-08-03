import mysql.connector
import json


class CustomJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, bytes):
            return o.decode('utf-8')  # Convert bytes to str
        return super().default(o)


def get_metadata(host, user, password, database):
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        metadata = {}
        cursor.execute("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY, \
                        COLUMN_DEFAULT, IS_NULLABLE, COLUMN_COMMENT \
                        FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s;", (database,))
        columns_info = cursor.fetchall()

        for table_name, column_name, column_type, column_key, column_default, is_nullable, column_comment in columns_info:
            if table_name not in metadata:
                metadata[table_name] = {}
            metadata[table_name][column_name] = {
                "data_type": column_type,
                "primary_key": True if column_key == 'PRI' else False,
                "foreign_key": None,  # Placeholder, we'll fill this later
                "default_value": column_default,
                "nullable": True if is_nullable == 'YES' else False,
                "comment": column_comment
            }

        # Get foreign keys and their relations
        cursor.execute("SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, \
                        REFERENCED_COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
                        WHERE TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME IS NOT NULL;", (database,))
        foreign_keys_info = cursor.fetchall()

        for table_name, column_name, referenced_table_name, referenced_column_name in foreign_keys_info:
            if table_name in metadata and column_name in metadata[table_name]:
                metadata[table_name][column_name]["foreign_key"] = {
                    "referenced_table": referenced_table_name,
                    "referenced_column": referenced_column_name
                }

        # Create table relations
        relations = {}
        for table_name, table_metadata in metadata.items():
            for column_name, column_info in table_metadata.items():
                if column_info["foreign_key"]:
                    referenced_table = column_info["foreign_key"]["referenced_table"]
                    referenced_column = column_info["foreign_key"]["referenced_column"]
                    if referenced_table not in relations:
                        relations[referenced_table] = {}
                    relations[referenced_table][referenced_column] = {
                        "table": table_name,
                        "column": column_name
                    }

        return metadata, relations

    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return None

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    host = 'localhost'
    user = 'root'
    password = 'root'
    database = 'sakila'

    metadata, relations = get_metadata(host, user, password, database)

    if metadata and relations:
        metadata_json = json.dumps(metadata, indent=2, cls=CustomJsonEncoder)
        relations_json = json.dumps(relations, indent=2, cls=CustomJsonEncoder)

        print("Table Metadata:")
        print(metadata_json)
        print("\nTable Relations:")
        print(relations_json)
