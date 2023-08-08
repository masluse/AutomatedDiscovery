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
        action = request.form.get('action')
        db_type = request.form.get('db_type')

        if db_type == "1":

            metadata, relations = openai_api_V5.get_metadata_mysql(host, user, password, database)
        
        if db_type == "2":

            metadata, relations = openai_api_V5.get_metadata_mssql(host, user, password, database)
        
        if db_type == "3":

            metadata, relations = openai_api_V5.get_metadata_postgresql(host, user, password, database)

        
        api_answer = openai_api_V5.get_queries(metadata, relations, first_name, last_name, action)
        
        if action == "1":

            if db_type == "1":

                results = openai_api_V5.run_query_mysql(host, user, password, database, api_answer)

            if db_type == "2":

                results = openai_api_V5.run_query_mssql(host, user, password, database, api_answer)

            if db_type == "3":

                results = openai_api_V5.run_query_postgresql(host, user, password, database, api_answer)
            
            query_results = list(zip(api_answer, results))
        
            return render_template('results1.html', query_results=query_results)
        
        if action == "2":

            return render_template('results2.html', sql_queries=api_answer)
    
    return render_template('index.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
