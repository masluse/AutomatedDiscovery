import mysql.connector
import json
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.cluster import KMeans
import numpy as np


def convert_bytes_to_string(obj):
    if isinstance(obj, bytes):
        return obj.decode('utf-8')
    raise TypeError


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

            for column_info in columns:
                column_info_dict = dict(zip(column_names, column_info))
                column_name = column_info_dict['Field']
                column_type = column_info_dict['Type']

                table_metadata[column_name] = column_type

            metadata[table_name] = table_metadata

        cursor.close()
        connection.close()

        return metadata

    except Exception as e:
        print(f"Error: {str(e)}")
        return None


def convert_metadata_to_json(metadata):
    return json.dumps(metadata, indent=4, default=convert_bytes_to_string)


def find_person_columns(metadata, num_clusters=2):
    column_names = []
    for table_name, table_metadata in metadata.items():
        column_names.extend(table_metadata.keys())

    vectorizer = CountVectorizer()
    X = vectorizer.fit_transform(column_names)

    kmeans = KMeans(n_clusters=num_clusters, random_state=42)
    kmeans.fit(X)

    # Getting the cluster centers (representative column names) and their indices
    cluster_centers = vectorizer.inverse_transform(kmeans.cluster_centers_)
    cluster_indices = np.argsort(kmeans.cluster_centers_.sum(axis=1))

    clusters = {}
    for idx in cluster_indices:
        representative_columns = cluster_centers[idx].tolist()
        clusters[idx] = representative_columns

    return clusters



if __name__ == "__main__":

    # Shared
    host = "localhost"
    user = "root"
    password = "root"

    # Database 1
    database1 = "classicmodels"

    metadata1 = get_database_metadata(host, user, password, database1)

    # Database 2
    database2 = "sakila"

    metadata2 = get_database_metadata(host, user, password, database2)

    # Database 3
    database3 = "world_x"

    metadata3 = get_database_metadata(host, user, password, database3)

    combined_metadata = {**metadata1, **metadata2, **metadata3}

    json_string_combined = convert_metadata_to_json(combined_metadata)
    print("Combined Metadata:")
    print(json_string_combined)

    num_clusters = 5
    clusters_combined = find_person_columns(combined_metadata, num_clusters=num_clusters)
    print(f"Found {len(clusters_combined)} clusters in the combined metadata:")
    for cluster_idx, representative_columns in clusters_combined.items():
        print(f"Cluster {cluster_idx}:")
        print(representative_columns)
        print("\n")
