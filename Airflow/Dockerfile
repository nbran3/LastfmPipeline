# Use the official Apache Airflow image
FROM apache/airflow:3.2.0-python3.11

# Set the working directory
WORKDIR /app

# Copy requirements.txt to the container
COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip

# Install additional Python packages from requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

