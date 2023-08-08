# We start with a base image that contains Python
FROM python:3.8-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the project files into the working directory
COPY . .

# Install the necessary dependencies
RUN pip install --upgrade pip
RUN pip install psycopg2-binary
RUN apt-get update && apt-get install libpq-dev -y
RUN pip install flask openai mysql-connector-python pymysql prettytable pyodbc psycopg2

# Add Microsoft's repo for the ODBC Driver 17 for SQL Server
RUN apt-get update && apt-get install -y gnupg2 curl && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

# Install the ODBC Driver 17 for SQL Server
RUN apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17

# We specify the port on which the server runs (Flask runs on port 5000 by default)
EXPOSE 5000

# This command is executed when the container starts
CMD ["python", "./app.py"]