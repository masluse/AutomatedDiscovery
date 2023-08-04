import mysql.connector
import json


class CustomJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, bytes):
            return o.decode('utf-8')  # Convert bytes to str
        return super().default(o)


def get_metadata(host, user, password, database):
    try:
        db = mysql.connector.connect(host=host, user=user, password=password, db=database)
        cursor = db.cursor()

        # Get the metadata
        cursor.execute(
            "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_KEY, IS_NULLABLE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s",
            (database,))
        rows = cursor.fetchall()


        metadata = {}
        for row in rows:
            table_name = row[0]
            column_name = row[1]
            primary_key = row[2] == 'PRI'
            nullable = row[3] == 'YES'
            data_type = row[4]

            if table_name not in metadata:
                metadata[table_name] = {}

            metadata[table_name][column_name] = {
                "type": data_type,
                "pk": primary_key,
                "nullable": nullable
            }

        # Get the relations
        cursor.execute("""
            SELECT 
                TABLE_NAME, 
                COLUMN_NAME, 
                REFERENCED_TABLE_NAME, 
                REFERENCED_COLUMN_NAME 
            FROM 
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
            WHERE 
                TABLE_SCHEMA = %s AND 
                REFERENCED_TABLE_NAME IS NOT NULL
        """, (database,))
        rows = cursor.fetchall()

        relations = {}
        for row in rows:
            table_name = row[0]
            column_name = row[1]
            ref_table_name = row[2]
            ref_column_name = row[3]

            if table_name not in relations:
                relations[table_name] = {}

            relations[table_name][column_name] = {
                "table": ref_table_name,
                "column": ref_column_name
            }

        return metadata, relations

    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return None, None

    finally:
        if db:
            db.close()


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
