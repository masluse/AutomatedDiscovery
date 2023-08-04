# We start with a base image that contains Python
FROM python:3.8-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the project files into the working directory
COPY . .

# Install the necessary dependencies
RUN pip install --upgrade pip
RUN pip install flask openai mysql-connector-python pymysql prettytable

# We specify the port on which the server runs (Flask runs on port 5000 by default)
EXPOSE 5000

# This command is executed when the container starts
CMD ["python", "./app.py"]
