import mysql.connector
import json

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from itertools import combinations


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


def find_person_columns(metadata, similarity_threshold=0.9):
    column_names = []
    for table_name, table_metadata in metadata.items():
        column_names.extend(table_metadata.keys())

    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(column_names)

    # Calculate cosine similarity matrix
    similarity_matrix = cosine_similarity(X)

    # Finding similar columns based on cosine similarity
    similar_columns = []
    n_columns = len(column_names)
    for i, j in combinations(range(n_columns), 2):
        if similarity_matrix[i, j] > similarity_threshold:
            similar_columns.append((i, j))

    # Create clusters based on similarity
    clusters = {}
    for i, j in similar_columns:
        cluster_found = False
        for cluster_idx, cluster_columns in clusters.items():
            if i in cluster_columns or j in cluster_columns:
                clusters[cluster_idx].add(i)
                clusters[cluster_idx].add(j)
                cluster_found = True
                break
        if not cluster_found:
            clusters[len(clusters)] = {i, j}

    # Convert indices back to column names
    cluster_results = {}
    for cluster_idx, cluster_indices in clusters.items():
        cluster_results[cluster_idx] = [column_names[idx] for idx in cluster_indices]

    return cluster_results



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

    similarity_threshold = 0.8
    clusters_combined = find_person_columns(combined_metadata, similarity_threshold=similarity_threshold)
    print(f"Found {len(clusters_combined)} similarities in the combined metadata:")
    for cluster_idx, representative_columns in clusters_combined.items():
        print(f"Cluster {cluster_idx}:")
        print(representative_columns)
        print("\n")

