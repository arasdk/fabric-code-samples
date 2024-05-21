# Loading Dynamics 365 Data into a Fabric Lakehouse Using Synapse Link
<br>

![Synapse Link drawio](https://github.com/arasdk/fabric-code-samples/assets/145650154/f8407e69-73a5-4a6e-867f-623a4f556d23)


## Overview
This notebook performs incremental merges of CSV files written by the Synapse Link trickle-feed to an ADLS storage account using Microsoft Fabric and PySpark. The process handles schema evolution, supports parallel merges, and ensures only the latest records are kept.

- The notebook is fully self-contained and needs only the configuration provided by the parameters.
- It can be executed by a Fabric Pipeline or scheduled directly, although the Pipeline option might be better to avoid stacking multiple notebook executions.
- The notebook will process change folders chronologically until it reaches the current folder.
- It will track the last folder processed by placing a watermark file specified via the "incremental_merge_folder" parameter. The watermark file contains a text string equal to the last processed folder name.
- It can run on any spark starter pool in Fabric and doesn't need external libraries installed.
- It will automatically detect which tables to import as long as the synapse link shortcut is pointing at the root of the ADLS storage account container created by Synapse Link.
- The notebook uses soft-deletes, which means that records marked as deleted by Synapse Link will be marked as deleted in the Fabric table via the column IsDelete = 1.
- If an error occurs for a given folder the script will abort and retry the folder the next time the notebook is run
- Log messages are outputted to console and <incremental_merge_folder>/logs

<br>

**Disclaimer**: The Fabric notebook provided in this repository is for demonstration purposes only. Before deploying it in your environment, it is highly recommended to thoroughly verify and enhance the notebook to ensure it meets your specific requirements for correctness, resiliency, and performance for your intended workload.

<br>

### Notebook Parameters
- **lakehouse_name**: The name of the lakehouse where the Synapse Link incremental feed is available as a shortcut.
- **incremental_merge_folder**: The folder in the lakehouse used for logs and watermarks.
- **synapse_link_shortcut_path**: The path to the shortcut pointing to the ADLS storage account folder containing the incremental CSV files from Synapse Link.
- **batch_size**: Defines how many table merges are parallelized. Set this value to 1 to perform a single merge at a time. 

The notebook can be parametarized by placing the following code section in the first notebook cell and configure it as a "Parameter" cell:
<br>

```python
# Lakehouse and folder configurations
lakehouse_name = "<lakehouse-name>"
incremental_merge_folder = "Files/synapse_link/incremental_merge_state"
synapse_link_shortcut_path = "Files/synapse_link/<your-shortcut-name>"
batch_size = 4
```

### Spark configuration Settings
These are the spark settings used by the script:
```
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
spark.conf.set("spark.ms.autotune.enabled", "true")
```

<br>

## Prerequisites
The following prerequisites are required to run the script.

<br>

### Dynamics 365 Finance and Operations
Microsoft lists the supported versions of D365FO as part of their Synapse Link documentation.
https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-select-fno-data

<br>

### ADLS Gen 2 Storage Account
Microsoft has written a nice little guide on how to setup a data lake for Synapse Link, which you can find here: 
[Create an Azure Synapse Link for Dataverse with Azure Data Lake](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-data-lake).

<br>

### Synapse Link Profile Setup
When [configuring your Synapse Link profile by selecting D365FO tables or entities](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-select-fno-data), you can ignore the options for "Append-only" and "Partition" strategies. Once the profile is saved, "Append-only" will automatically be set to read-only and will have a checkmark next to it. Similarly, "Partition" will be set to "Year."

Once the tables an entities has been selected and the profile has been saved, Synapse Link will start the proces of initializing the feed. This can take anywhere from hours to even days until all tables and entities are sync'ed to the data lake.

<br>

### Prepare a Fabric Lakehouse
When you have created a Fabric Lakehouse to hold your tables from Synapse Link, you need to add a shortcut to the ADLS storage account. 

- Go to the "Files" section of your Lakehouse, right-click and select "New folder". Give the folder the name synapse_link.
- Next, right-click on the synapse_link folder and select "New shortcut". 
- From the shortcut wizard navigate to the ADLS storage account and select the container created by Synapse Link for your Dataverse environment. The container should be named dataverse-environment-name-environment uniqe id. Note down the name given to the shortcut as it will be needed later on. 
- Create a Fabric notebook in the same workspace as the Lakehouse and insert the source code from the git repo script. Important: Make sure the Lakehouse is added to your notebook and configured as the default Lakehouse. You can verify that the Lakehouse is the default Lakehouse when a "pin" icon is displayed next to the Lakehouse with the notebook opened. If this step is omitted the script will not be able to take advantage of the Lakehouse default mount point (i.e. "/lakehouse/default/Files/...") for the "Files" folder when running the spark job.

<br>
