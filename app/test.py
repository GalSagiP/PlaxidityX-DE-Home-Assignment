import json
from pyspark.sql.functions import col, count, when
from pyspark.sql import DataFrame
from utils import read_json

def load_schema(config_path: str, table_name: str):
    """
    Load expected schema (column names, types, and key columns) for a given table from JSON.

    :param config_path: Path to the schema JSON file.
    :param table_name: Name of the table to extract schema for.
    :return: expected_columns, expected_types, key_columns
    """
    schema_config = read_json(config_path)

    if table_name not in schema_config:
        raise ValueError(f"Table '{table_name}' not found in JSON")

    table_schema = schema_config[table_name]
    return table_schema["expected_columns"], table_schema["expected_types"], table_schema["key_columns"]


def test_dataframe(df: DataFrame, schema_json_path: str, table_name: str):
    """
    Run schema validation tests on a Spark DataFrame.

    :param df: DataFrame to test.
    :param schema_json_path: Path to schema JSON file.
    :param table_name: Name of the table being tested.
    """
    expected_columns, expected_types, key_columns = load_schema(schema_json_path, table_name)

    # Validate Column Names
    actual_columns = df.columns
    assert sorted(actual_columns) == sorted(expected_columns), f"Column names mismatch! Expected: {expected_columns}, Found: {actual_columns}"
    print("Column names are correct")

    # Check for Null Values
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
    for column, null_count in null_counts.items():
        assert null_count == 0, f"Null values found in column '{column}'"
    print("No null values in any columns")

    # Validate Column Data Types
    actual_types = dict(df.dtypes)
    for column, expected_type in expected_types.items():
        assert actual_types[column] == expected_type, f"Column '{column}' type mismatch! Expected: {expected_type}, Found: {actual_types[column]}"
    print("Column data types are correct")

    # Check for Duplicate Keys
    df_no_duplicates = df.dropDuplicates(key_columns)
    assert df_no_duplicates.count() == df.count(), "Duplicate keys found!"
    print("No duplicates found in key columns.")

    print(f"All tests passed for table: {table_name}")
