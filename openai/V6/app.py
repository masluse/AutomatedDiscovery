from flask import Flask, request, render_template
import openai_api_V5  # Importieren Sie das Python-Skript, das Sie erstellt haben.

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        host = request.form.get('host')
        user = request.form.get('user')
        password = request.form.get('password')
        database = request.form.get('database')
        first_name = request.form.get('first_name')
        last_name = request.form.get('last_name')
        
        # Extrahieren Sie die Metadaten und Beziehungen aus der Datenbank.
        metadata, relations = openai_api_V5.get_metadata(host, user, password, database)
        
        # Generieren Sie die SQL-Abfragen.
        sql_queries = openai_api_V5.get_queries(metadata, relations, first_name, last_name)
        
        # Führen Sie die Abfragen aus und erhalten Sie die Ergebnisse.
        results = openai_api_V5.run_query(host, user, password, database, sql_queries)

        # Zusammenführen der queries und results in einer Liste von Tupeln
        query_results = list(zip(sql_queries, results))
        
        return render_template('results.html', query_results=query_results)
    
    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=True)
