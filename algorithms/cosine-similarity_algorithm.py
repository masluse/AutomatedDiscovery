import mysql.connector
import json

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from itertools import combinations

from extract_metadata import get_metadata


def convert_bytes_to_string(obj):
    if isinstance(obj, bytes):
        return obj.decode('utf-8')
    return obj


def convert_metadata_to_json(metadata):
    return json.dumps(metadata, indent=4, default=convert_bytes_to_string)


def find_person_columns(metadata, relations, similarity_threshold=0.9):
    column_names = []
    for table_name, table_metadata in metadata.items():
        for column_name, column_data in table_metadata.items():
            if not column_data['pk']:  # Exclude primary keys from similarity analysis
                column_names.append(column_name)

    # Add related column names to the list
    for table_name, table_relations in relations.items():
        for _, relation_data in table_relations.items():
            column_names.append(relation_data['column'])

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

    metadata1, relations1  = get_metadata(host, user, password, database1)

    # Database 2
    database2 = "sakila"

    metadata2, relations2 = get_metadata(host, user, password, database2)

    # Database 3
    database3 = "world_x"

    metadata3, relations3 = get_metadata(host, user, password, database3)

    combined_metadata = {**metadata1, **metadata2, **metadata3}
    combined_relations = {**relations1, **relations2, **relations3}

    json_string_combined = convert_metadata_to_json(combined_metadata)
    print("Combined Metadata:")
    print(json_string_combined)

    similarity_threshold = 0.8
    clusters_combined = find_person_columns(combined_metadata, combined_relations,
                                            similarity_threshold=similarity_threshold)
    print(f"Found {len(clusters_combined)} similarities in the combined metadata:")
    for cluster_idx, representative_columns in clusters_combined.items():
        print(f"Cluster {cluster_idx}:")
        print(representative_columns)
        print("\n")
