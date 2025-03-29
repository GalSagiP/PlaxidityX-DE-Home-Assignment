import logging
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import os
import shutil
from pyspark.sql import DataFrame
import json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "etl-bucket")

# MySQL Configuration
MYSQL_URL = os.getenv("MYSQL_URL", "jdbc:mysql://mysql:3306/lahman2016")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_from_mysql(spark: SparkSession, query: str):
    """
    Read data from MySQL using JDBC with a specified query.

    :param spark: SparkSession instance.
    :param query: SQL query to fetch required data.
    :return: Spark DataFrame.
    :raises ConnectionError: If unable to connect to MySQL.
    :raises ValueError: If the query fails or returns empty.
    """
    try:
        df = spark.read.format("jdbc").options(
            url=MYSQL_URL,
            driver=MYSQL_DRIVER,
            dbtable=query,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        ).load()

        if df.isEmpty():
            raise ValueError("The query returned an empty dataset.")

        logger.info("Successfully loaded data")
        return df

    except AnalysisException as e:
        logger.error(f"Error during query execution: {str(e)}")
        raise ValueError(f"Error executing query: {query}. Check the SQL syntax or database connection.") from e
    except Exception as e:
        logger.error(f"Error while reading data from MySQL: {str(e)}")
        raise ConnectionError(f"Failed to connect to MySQL or execute the query: {query}") from e


def save_to_minio(df, file_name) -> None:
    """
    Save a Spark DataFrame to MinIO as a CSV file.

    :param df: Spark DataFrame to save.
    :param file_name: Name of the CSV file to be saved.
    :raises FileNotFoundError: If the destination path is incorrect or not accessible.
    :raises PermissionError: If there are permission issues with the MinIO bucket.
    :raises Exception: For general failures during file saving.
    """
    try:
        minio_path = f"{MINIO_ENDPOINT}/{MINIO_BUCKET}/{file_name}"

        # Try saving the DataFrame to MinIO as a CSV
        df.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(minio_path)

        logger.info(f"Saved {file_name} to MinIO: {minio_path}")

    except FileNotFoundError as e:
        logger.error(f"File path error: {str(e)}")
        raise FileNotFoundError(f"The file path {minio_path} does not exist or is incorrect.") from e
    except PermissionError as e:
        logger.error(f"Permission error: {str(e)}")
        raise PermissionError(
            f"Permission denied while saving to {minio_path}. Check your MinIO credentials and access rights.") from e
    except Exception as e:
        logger.error(f"Error saving to MinIO: {str(e)}")
        raise Exception(f"Failed to save {file_name} to MinIO at {minio_path}.") from e


def write_single_csv(df: DataFrame, output_dir: str, file_name: str):
    """
    Write the DataFrame to a single CSV file with a specified name.

    :param df: The DataFrame to write.
    :param output_dir: The directory where the CSV file will be saved.
    :param file_name: The name of the output CSV file.
    """

    # Temporary write directory
    temp_output_dir = output_dir + "_temp"

    # Write the DataFrame to a temporary directory as a single CSV file
    df.coalesce(1).write.mode("overwrite").csv(temp_output_dir, header=True)

    # Ensure output directory exists only if data was written
    if not os.path.exists(temp_output_dir):
        print(f"Error: Temporary output directory '{temp_output_dir}' was not created.")
        return

    os.makedirs(output_dir, exist_ok=True)  # Create only if necessary

    # Find and rename the CSV file
    for filename in os.listdir(temp_output_dir):
        if filename.startswith("part-") and filename.endswith(".csv"):
            shutil.move(os.path.join(temp_output_dir, filename), os.path.join(output_dir, file_name))
            break

    # Remove temporary directory and any Spark metadata files
    shutil.rmtree(temp_output_dir)
    for filename in os.listdir(output_dir):
        if filename in ["_SUCCESS", "_START", "_metadata", "_temporary"]:
            os.remove(os.path.join(output_dir, filename))

    print(f">>>>>>>>>>>>>> CSV file saved as: {os.path.join(output_dir, file_name)}")


def read_json(json_path):
    with open(json_path, "r") as f:
        return json.load(f)


def load_schema_from_json(json_path, table_name):
    schema_data = read_json(json_path)

    expected_types = schema_data[table_name]["expected_types"]

    fields = []
    for column, col_type in expected_types.items():
        # Map the string type to actual PySpark types
        if col_type == "int":
            pyspark_type = IntegerType()
        elif col_type == "string":
            pyspark_type = StringType()
        elif col_type == "double":
            pyspark_type = DoubleType()
        elif col_type == "bigint":
            pyspark_type = LongType()
        else:
            raise ValueError(f"Unsupported type: {col_type}")

        # Add the StructField for each column
        fields.append(StructField(column, pyspark_type, nullable=False))

    return StructType(fields)
