# coding: utf-8
# synapse_link_adls_incremental_merge

# In[1]:

# NOTE: This notebook requires that the lakehouse containing the shortcut to the ADLS Gen2 Storage Account
# with the incremental CSV files is added to the notebook and configured (pinned) as the default lakehouse.

# The path to the shortcut that points to the storage account folder where the incremental timestamp folders are located
# Change this path to the correct path for your environment
synapse_link_shortcut_path = "Files/synapse_link"

# This folder is placed in the default lakehouse and used for logs and watermarks
merge_state_path = "Files/incremental_merge_state"

# Specify the number of processed timestamp folders to keep before moving them to the archive folder
# If the number of timestamp folders are large, archiving may reduce the time it takes to list folder content
# Set to 0 to ignore archiving
# NOTE: Archiving requires storage blob data contributor role on the ADLS Gen2 Storage Account
max_timestamps_before_archive = 0
timestamp_archive_path = f"{synapse_link_shortcut_path}/archive"

# Define the schema to use for incremental tables. Set to "" for lakehouses with no schema support
table_schema = "dbo"

# Define the batch size to control how many table merges are parallelized
# Set to 1 (sequential merging) when using a small Fabric capacity or to reduce capacity utilization
batch_size = 4


# In[2]:


import concurrent.futures
import json
import logging
import time
import re
from notebookutils import mssparkutils
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    row_number,
    desc,
    lit,
    to_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    DoubleType,
    BooleanType,
    DecimalType,
)
from pyspark.sql.window import Window

# Merge can lead to "ancient" timestamp errors unless int96RebaseModeInWrite is set to CORRECTED
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

# Schema evolution when merging
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

# https://learn.microsoft.com/en-us/fabric/data-engineering/autotune?tabs=pyspark
spark.conf.set("spark.ms.autotune.enabled", "true")

# Create lakehouse schema if it doesn't exist
if table_schema:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {table_schema}")


# Set up logging
# Loggs will be appended to files YYYYMMDD.log files
date_str = time.strftime("%Y%m%d")
log_path = f"{merge_state_path}/Logs"
file_name = f"{date_str}.log"
mssparkutils.fs.mkdirs(log_path)

logging.basicConfig(
    filename=f"/lakehouse/default/{log_path}/{file_name}",
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True,
)

# Create a console handler so we can see log messages directly in Notebook output
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_format)
logging.getLogger().addHandler(console_handler)


# In[3]:


# Utility methods


def save_last_processed_folder(folder):
    mssparkutils.fs.put(f"{merge_state_path}/.last_processed_folder", folder, True)


def get_last_processed_folder() -> str:
    if mssparkutils.fs.exists(f"{merge_state_path}/.last_processed_folder"):
        return mssparkutils.fs.head(f"{merge_state_path}/.last_processed_folder", 1024)
    return None


def archive_folder(folder):
    source_folder = f"{synapse_link_shortcut_path}/{folder}"
    archive_folder = f"{timestamp_archive_path}/{folder}"
    logging.info(f"Archiving {source_folder} to '{archive_folder}'")
    mssparkutils.fs.mv(source_folder, archive_folder, True)


def apply_custom_formatting(df):
    custom_format = "MM/d/yyyy h:mm:ss a"
    if "SinkCreatedOn" in df.columns:
        df = df.withColumn(
            "SinkCreatedOn", to_timestamp(col("SinkCreatedOn"), custom_format)
        )
    if "SinkModifiedOn" in df.columns:
        df = df.withColumn(
            "SinkModifiedOn", to_timestamp(col("SinkModifiedOn"), custom_format)
        )

    return df


# Returns a StructField object based on the provided field name and data type.
def get_struct_field(field_name, data_type) -> StructField:
    if data_type == "string":
        return StructField(field_name, StringType(), True)
    elif data_type == "guid":
        return StructField(field_name, StringType(), True)
    elif data_type == "int64":
        return StructField(field_name, LongType(), True)
    elif data_type == "int32":
        return StructField(field_name, IntegerType(), True)
    elif data_type == "dateTime":
        if field_name in ["SinkCreatedOn", "SinkModifiedOn"]:
            # Handle custom datetime format for SinkCreatedOn and SinkModifiedOn
            return StructField(field_name, StringType(), True)
        else:
            return StructField(field_name, TimestampType(), True)
    elif data_type == "dateTimeOffset":
        return StructField(field_name, TimestampType(), True)
    elif data_type == "decimal":
        return StructField(field_name, DecimalType(38, 18), True)
    elif data_type == "double":
        return StructField(field_name, DoubleType(), True)
    elif data_type == "boolean":
        return StructField(field_name, BooleanType(), True)
    else:
        return StructField(field_name, StringType(), True)


# Reads the model schema file from the given URL and returns a dictionary representing the table schema and partitions.
# Returns adictionary representing the table schema and partitions, where the keys are table names and the values are
# the corresponding table schema/partitions.
def get_table_schema_dict(folder) -> {}:
    # Load the folder-specific  model.json
    schema_file = f"/lakehouse/default/{synapse_link_shortcut_path}/{folder}/model.json"
    with open(schema_file, "r", encoding="utf-8") as f:
        schema = json.load(f)

    # Build the schema dict
    table_schema_dict = {}

    # Only include tables that includes partitions
    # Tables with no listed partitions have not exported any files in the given folder
    for table in schema["entities"]:
        if "partitions" in table and table["partitions"]:

            table_name = None
            table_schema = StructType()
            for table_attribute in table["attributes"]:

                # New table name? Add the previous table schema to dict
                if table_name is not None and table_name != table["name"]:
                    schema_and_partitions_dict = {
                        "schema": table_schema,
                        "partitions": table["partitions"],
                    }
                    table_schema_dict[table_name] = schema_and_partitions_dict
                    table_schema = StructType()

                table_name = table["name"]
                table_schema.add(
                    get_struct_field(
                        table_attribute["name"], table_attribute["dataType"]
                    )
                )

            # Process the last table after the loop exits
            schema_and_partitions_dict = {
                "schema": table_schema,
                "partitions": table["partitions"],
            }

            table_schema_dict[table_name] = schema_and_partitions_dict

    return table_schema_dict


# We use the persisted last_merge_timestamp from the table to determine the starting point
def get_folders():
    folders = []
    last_processed_folder = get_last_processed_folder()

    # Regular expression pattern to match the timestamp format
    # ISO 8601 date and time representation
    timestamp_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}\.\d{2}\.\d{2}Z$")

    files = mssparkutils.fs.ls(synapse_link_shortcut_path)

    # Extract folders with timestamp names
    timestamp_folders = [
        file.name for file in files if timestamp_pattern.match(file.name)
    ]

    # Sort the list in ascending order
    # ISO 8601 format, such as YYYY-MM-DDTHH:MM:SSZ, is lexicographically sortable
    timestamp_folders_sorted = sorted(timestamp_folders)

    # Skip the last folder since that is the current (active one)
    folders_to_process = timestamp_folders_sorted[:-1]

    # Loop through the timestamp folders and add folder if it has not previously been merged
    # Check if the folder contains model.json and if model.json > 0B
    for timestamp_folder in folders_to_process:
        if last_processed_folder is None or timestamp_folder > last_processed_folder:

            schema_file = f"{synapse_link_shortcut_path}/{timestamp_folder}/model.json"
            if mssparkutils.fs.exists(schema_file) and mssparkutils.fs.head(
                schema_file, 100
            ):
                folders.append(timestamp_folder)

        elif max_timestamps_before_archive > 0:
            # Check if timestmap folder should be archived
            remaining_folders = folders_to_process[
                folders_to_process.index(timestamp_folder) + 1 :
            ]
            if len(remaining_folders) >= max_timestamps_before_archive:
                # Move the processed folder to an archive location
                archive_folder(timestamp_folder)

    # Return the folders list without the last (currently active) folder
    return folders


# In[4]:


# Merge logic


def merge_incremental_table(table_name, folder, schema_and_partitions_dict):
    for partition in schema_and_partitions_dict["partitions"]:
        merge_incremental_csv_file(
            table_name, folder, partition["name"], schema_and_partitions_dict["schema"]
        )


def merge_incremental_csv_file(table_name, folder, partition_name, schema):
    # Conditionally include table_schema if it is not None or an empty string
    fabric_table_path = (
        f"Tables/{table_schema + '/' if table_schema else ''}{table_name}"
    )
    fabric_table_name = f"{table_schema + '.' if table_schema else ''}{table_name}"
    partition_file = f"{folder}/{table_name}/{partition_name}.csv"
    table_partition_file = f"{synapse_link_shortcut_path}/{partition_file}"

    logging.info(f"Loading csv data from {partition_file}..")

    csv_data_df = (
        spark.read.format("csv")
        .option("header", False)
        .option("multiline", True)
        .option("delimiter", ",")
        .option("quote", '"')
        .option("escape", "\\")
        .option("escape", '"')
        .schema(schema)
        .load(table_partition_file)
    )

    # We need to ensure only the latest record for a given id is present in source df or we will get ambiguity errors.
    # Define the window specification with partition by 'id' and order by 'IsDelete' and 'sysrowversion' descending.
    # sysrowversion is null for deletes, so we ensure that the deletion change takes priority
    windowSpec = Window.partitionBy("id").orderBy(
        desc("IsDelete"), desc("sysrowversion")
    )

    # Add a row number over the window specification
    df_with_row_number = csv_data_df.withColumn(
        "merge_row_number", row_number().over(windowSpec)
    )

    # Filter out only the rows with row_number = 1; these are the latest relevant records per id
    latest_records_df = df_with_row_number.filter(
        df_with_row_number.merge_row_number == 1
    ).drop("merge_row_number")

    final_df = apply_custom_formatting(latest_records_df)

    # Does the table exist? If not create it from our df.
    # If it exists, proceed with merge instead
    if not DeltaTable.isDeltaTable(spark, fabric_table_path):
        # Create:
        final_df.write.mode("overwrite").format("delta").saveAsTable(
            f"{fabric_table_name}"
        )
        logging.info(f"Created table {fabric_table_name}")
    else:
        # Merge:

        logging.info(f"Merging {partition_file} to '{fabric_table_name}'")
        destination_table = DeltaTable.forPath(spark, fabric_table_path)

        # Only update records from sources that has a newer sysrowversion (thus skipping over previously processed records)
        # Important note: sysrowversion is not provided for deleted records, so deleted records should always update when matched.
        destination_table.alias("destination").merge(
            final_df.alias("source"), "source.id = destination.id"
        ).whenMatchedUpdateAll(
            condition="source.IsDelete = 1 OR source.sysrowversion > destination.sysrowversion"
        ).whenNotMatchedInsertAll().execute()


# In[5]:


# Iterate new folders and merge
folders = get_folders()

with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
    for folder in folders:
        logging.info(f"Processing folder {folder}..")

        # Load the correct schema matching the change folder
        table_schema_dict = get_table_schema_dict(folder)
        table_names = [key for key in table_schema_dict.keys()]

        # Submit merge command(s) to the executor
        futures = [
            executor.submit(
                merge_incremental_table,
                table_name,
                folder,
                table_schema_dict[table_name],
            )
            for table_name in table_names
        ]

        # Check the results of the futures
        try:
            # This will raise an exception if the callable raised one
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
        except Exception as e:
            logger.critical(f"Error on {folder}/{table_name}: %s", e, exc_info=True)
            raise e

        # All tables merged for the folder
        # Update last_processed_folder watermark
        save_last_processed_folder(folder)


logging.info("Merge complete")
