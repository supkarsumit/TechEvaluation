"""
ETL Assignment

Complete all TODO sections.
This script should:
  1. Read a CSV file into a DataFrame from AWS S3
  2. Clean and transform the data
  3. Write the result as partitioned Parquet files
"""

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
# TODO: AWS SDk library
from pathlib import Path


def extract(input_path: str) -> pd.DataFrame:
    """
    Load the CSV into a pandas DataFrame.
    """
    # TODO: Load the CSV file using pandas
    df = None

    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all required transformations:
      - Trim text columns
      - Convert types (quantity, price, date)
      - Handle missing values
      - Add total_value_usd
      - Add year_month column
      - Filter out zero-value rows
    """
    # TODO: Trim text fields
    # Example:
    # df["product"] = ...

    # TODO: Normalise category (title case)

    # TODO: Convert quantity column to int, replace missing values with 0

    # TODO: Convert unit_price_usd to float

    # TODO: Convert transaction_date to datetime

    # TODO: Create total_value_usd = quantity * unit_price_usd

    # TODO: Create year_month column (YYYY-MM from transaction_date)

    # TODO: Filter out rows with total_value_usd == 0

    return df


def load(df: pd.DataFrame, output_root: str):
    """
    Write the DataFrame as partitioned Parquet files.
    One folder per year_month.
    """
    output_path = Path(output_root)
    output_path.mkdir(parents=True, exist_ok=True)

    # TODO: Group by the partition column (year_month)
    # Example:
    # for partition_value, group in ...:

    # Within the loop:
    #   - Create a folder "year_month=<value>"
    #   - Convert the pandas DataFrame to a PyArrow table
    #   - Write as Parquet with Snappy compression
    #
    # TODO: Implement the partitioned write


def main():

    input_csv = "s3://etl-test-bucket-exercise/data/sales_data_file.csv"
    output_dir = "output_parquet"


    print("Extracting...")
    df = extract(input_csv)

    print("Transforming...")
    df_transformed = transform(df)

    print("Loading...")
    load(df_transformed, output_dir)

    print("ETL Complete.")


if __name__ == "__main__":

    main()
