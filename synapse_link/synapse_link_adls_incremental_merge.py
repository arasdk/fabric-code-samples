# coding: utf-8
# synapse_link_adls_incremental_merge

# In[1]:


# The name of the lakehouse in the current workspace where synapse link incremental feed is available as a shortcut
lakehouse_name = "<enter-lakehouse-name>"

# This folder is placed in the default lakehouse and used for logs and watermarks
incremental_merge_folder = "Files/synapse_link/incremental_merge_state"

# The path to the shortcut that points to the storage account folder where the incremental csv files are located
synapse_link_shortcut_path = "Files/synapse_link/<enter-shortcut-name>"

# Define the batch size to control how many table merges are parallelized
batch_size = 4


# In[2]:


import concurrent.futures
import json
import logging
import time
from notebookutils import mssparkutils
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    row_number,
    desc,
    lit,
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

# Resolve lakehouse and workspace id using mssparkutils
LAKEHOUSE_ID = mssparkutils.lakehouse.get(lakehouse_name).id
WORKSPACE_ID = mssparkutils.lakehouse.get(lakehouse_name).workspaceId

# changelog.info contains the active folder name
# Synapse.log contains a comma-separated list of previously processed folders ordered by date
CHANGELOG_URL = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/{synapse_link_shortcut_path}/Changelog/changelog.info"
SYNAPSELOG_URL = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/{synapse_link_shortcut_path}/Changelog/Synapse.log"

# This points to the fully qualified name of a given change folder
FOLDER_URL_TEMPLATE = "abfss://{0}@onelake.dfs.fabric.microsoft.com/{1}/{2}/{3}"

# This points to the folder that contains table specific csv files for a given change folder
TABLE_URL_TEMPLATE = "abfss://{0}@onelake.dfs.fabric.microsoft.com/{1}/{2}/{3}"

# Use mssparkutils to get the list of all processed folders.
# This saves having to perform a list operation on the storage account.
# Note: SYNAPSELOG folders are sorted by date, so the newest folder is last in the list
PREV_FOLDERS = mssparkutils.fs.head(SYNAPSELOG_URL, 1024 * 10000).split(",")

# Set up logging
# Loggs will be appended to files YYYYMMDD.log files
date_str = time.strftime("%Y%m%d")
log_path = f"{incremental_merge_folder}/Logs"
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
    mssparkutils.fs.put(
        f"{incremental_merge_folder}/.last_processed_folder", folder, True
    )


def get_last_processed_folder() -> str:
    if mssparkutils.fs.exists(f"{incremental_merge_folder}/.last_processed_folder"):
        return mssparkutils.fs.head(
            f"{incremental_merge_folder}/.last_processed_folder", 1024
        )
    return None


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
    # Load the folder-specific  model.json using the OS-level default lakehouse file mount
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

    # Loop through the PREV_FOLDERS in reverse, so we process the last folders first
    # Add folder if it has not previously been merged
    # The date format of the timestamp folders are lexicographically sortable, so we can
    # just compare our persisted folder name with the folder list to and add folders that are newer.
    for folder in reversed(PREV_FOLDERS):
        if last_processed_folder is None or folder > last_processed_folder:
            folders.append(folder)

    # Reverse the order of the result, so that folders are ordered chronologically
    folders.reverse()

    return folders


# In[4]:


# Merge logic


def merge_incremental_table(table_name, folder, schema_and_partitions_dict):
    for partition in schema_and_partitions_dict["partitions"]:
        merge_incremental_csv_file(
            table_name, folder, partition["name"], schema_and_partitions_dict["schema"]
        )


def merge_incremental_csv_file(table_name, folder, partition_name, schema):
    fabric_table_path = f"Tables/{table_name}_incremental"
    partition_file = f"{folder}/{table_name}/{partition_name}.csv"

    table_partition_file = TABLE_URL_TEMPLATE.format(
        WORKSPACE_ID, LAKEHOUSE_ID, synapse_link_shortcut_path, partition_file
    )

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

    # Does the table exist? If not create it from our df.
    # If it exists, proceed with merge instead
    if not DeltaTable.isDeltaTable(spark, fabric_table_path):
        # Create:
        latest_records_df.write.mode("overwrite").format("delta").saveAsTable(
            f"{table_name}_incremental"
        )
        logging.info(f"Created table {fabric_table_path}")
    else:
        # Merge:

        logging.info(f"Merging {partition_file} to '{fabric_table_path}'")
        destination_table = DeltaTable.forPath(spark, fabric_table_path)

        # Only update records from sources that has a newer sysrowversion (thus skipping over previously processed records)
        # Important note: sysrowversion is not provided for deleted records, so deleted records should always update when matched.
        destination_table.alias("destination").merge(
            latest_records_df.alias("source"), "source.id = destination.id"
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
