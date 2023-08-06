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

        metadata, relations = openai_api_V5.get_metadata(host, user, password, database)
        
        api_answer = openai_api_V5.get_queries(metadata, relations, first_name, last_name, action)
        
        if action == "1":

            results = openai_api_V5.run_query(host, user, password, database, api_answer)

            query_results = list(zip(api_answer, results))
        
            return render_template('results1.html', query_results=query_results)
        
        if action == "2":

            return render_template('results2.html', sql_queries=api_answer)
    
    return render_template('index.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
