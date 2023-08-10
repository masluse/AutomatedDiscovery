FROM python:3.8-slim-buster

WORKDIR /app

COPY ./src/main .

# Install system dependencies for pyodbc
RUN apt-get update && apt-get install -y gnupg2 curl unixodbc-dev

# Add Microsoft's repo for the ODBC Driver 17 for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

# Install the ODBC Driver 17 for SQL Server
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Install the necessary Python packages
RUN pip install --upgrade pip && \
    pip install psycopg2-binary flask openai mysql-connector-python pymysql prettytable pyodbc

EXPOSE 5000

CMD ["python", "/app/app.py"]
