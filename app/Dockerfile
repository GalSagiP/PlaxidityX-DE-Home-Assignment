# Use a Python 3.10 base image
FROM python:3.10-slim

# Install required dependencies for Spark and MySQL
RUN apt-get update && \
    apt-get install -y wget curl openjdk-17-jdk-headless && \
    apt-get clean

# Set Java environment variables for Spark compatibility
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install the MySQL JDBC driver for PySpark
RUN wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.29/mysql-connector-java-8.0.29.jar -P /opt/spark/jars/

# Install PySpark and other Python dependencies
RUN pip install pyspark==3.3.2 mysql-connector-python

# Set the working directory inside the container
WORKDIR /app

# Copy the ETL script into the container
COPY etl.py .

# Run the Python script
CMD ["python", "etl.py"]