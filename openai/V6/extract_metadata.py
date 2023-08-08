import mysql.connector
import json
import pyodbc
import psycopg2

class CustomJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, bytes):
            return o.decode('utf-8')  # Convert bytes to str
        return super().default(o)

def get_metadata_mssql(host, user, password, database):
    conn = None 
    try:
        conn_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={database};UID={user};PWD={password}"
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()

        # Get the metadata
        cursor.execute("""
            SELECT 
                SCHEMA_NAME = s.name,
                TABLE_NAME = t.name,
                COLUMN_NAME = c.name,
                COLUMN_KEY = CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'PRI' ELSE '' END,
                IS_NULLABLE = CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END,
                COLUMN_TYPE = ty.name
            FROM 
                sys.tables t
            INNER JOIN 
                sys.columns c ON t.object_id = c.object_id
            INNER JOIN 
                sys.schemas s ON t.schema_id = s.schema_id
            LEFT JOIN 
                sys.types ty ON c.system_type_id = ty.system_type_id
            LEFT JOIN (
                SELECT 
                    ku.TABLE_NAME, ku.COLUMN_NAME
                FROM 
                    INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
                INNER JOIN 
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                    ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
            ) AS pk
            ON  t.name = pk.TABLE_NAME AND c.name = pk.COLUMN_NAME
        """)
        rows = cursor.fetchall()

        metadata = {}
        for row in rows:
            schema_name = row.SCHEMA_NAME
            table_name = row.TABLE_NAME
            column_name = row.COLUMN_NAME
            primary_key = row.COLUMN_KEY == 'PRI'
            nullable = row.IS_NULLABLE == 'YES'
            data_type = row.COLUMN_TYPE

            full_table_name = f"{schema_name}.{table_name}"

            if full_table_name not in metadata:
                metadata[full_table_name] = {}

            metadata[full_table_name][column_name] = {
                "type": data_type,
                "pk": primary_key,
                "nullable": nullable
            }

        # Get the relations
        # NOTE: You might need to adjust this part as well to include schema information if required
        cursor.execute("""
            SELECT 
                FK.TABLE_NAME,
                CU.COLUMN_NAME,
                PK.TABLE_NAME AS REFERENCED_TABLE_NAME,
                PT.COLUMN_NAME AS REFERENCED_COLUMN_NAME
            FROM 
                INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C
            INNER JOIN 
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK
                ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME
            INNER JOIN 
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK
                ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME
            INNER JOIN 
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU
                ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME
            INNER JOIN (
                SELECT 
                    i1.TABLE_NAME, i2.COLUMN_NAME
                FROM 
                    INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1
                INNER JOIN 
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2
                    ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME
                WHERE 
                    i1.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ) PT
            ON PT.TABLE_NAME = PK.TABLE_NAME
        """)
        rows = cursor.fetchall()

        relations = {}
        for row in rows:
            table_name = row.TABLE_NAME
            column_name = row.COLUMN_NAME
            ref_table_name = row.REFERENCED_TABLE_NAME
            ref_column_name = row.REFERENCED_COLUMN_NAME

            if table_name not in relations:
                relations[table_name] = {}

            relations[table_name][column_name] = {
                "table": ref_table_name,
                "column": ref_column_name
            }

        return metadata, relations

    except pyodbc.Error as e:
        print(f"Error: {e}")
        return None, None

    finally:
        if conn:
            conn.close()

def get_metadata_postgresql(host, user, password, database):
    try:
        conn = psycopg2.connect(host=host, user=user, password=password, dbname=database)
        cursor = conn.cursor()

        # Get the metadata
        cursor.execute("""
            SELECT 
                table_name, column_name, column_default, is_nullable, data_type 
            FROM 
                information_schema.columns 
            WHERE 
                table_schema = 'public'
        """)
        rows = cursor.fetchall()

        metadata = {}
        for row in rows:
            table_name = row[0]
            column_name = row[1]
            primary_key = 'nextval' in (row[2] or '')
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
                kcu.table_name, 
                kcu.column_name, 
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM 
                information_schema.table_constraints AS tc 
            JOIN 
                information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN 
                information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE 
                constraint_type = 'FOREIGN KEY'
        """)
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

    except psycopg2.Error as e:
        print(f"Error: {e}")
        return None, None

    finally:
        if conn:
            conn.close()


def get_metadata_mysql(host, user, password, database):
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

    metadata, relations = get_metadata_mysql(host, user, password, database)

    if metadata and relations:
        metadata_json = json.dumps(metadata, indent=2, cls=CustomJsonEncoder)
        relations_json = json.dumps(relations, indent=2, cls=CustomJsonEncoder)

        print("Table Metadata:")
        print(metadata_json)
        print("\nTable Relations:")
        print(relations_json)
