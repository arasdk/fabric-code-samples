# Loading Incremental Changes from Dynamics 365 Finance and Operations Using Microsoft Fabric and Synapse Link
<br>

![Synapse Link drawio](https://github.com/arasdk/fabric-code-samples/assets/145650154/f8407e69-73a5-4a6e-867f-623a4f556d23)


## Overview
The Fabric notebook performs incremental merges of CSV files written by the Synapse Link trickle-feed to an ADLS storage account. It handles schema evolution, supports parallel merges, and ensures that only the latest records are kept.

Key features of the notebook include:

- **Self-Contained**: The notebook requires only the configuration provided by the notebook parameters.
- **Execution Options**: It can be executed by a Fabric Pipeline or scheduled directly. Using a Pipeline is recommended, because pipelines can be configured to prevent overlapping notebook executions
- **Chronological Processing**: The notebook processes change folders in chronological order until it reaches the current folder.
- **Watermark Tracking**: It tracks the last processed folder by placing a watermark file, specified via the "incremental_merge_folder" parameter. This file contains the name of the last processed folder.
- **Compatibility**: It can run on any Spark starter pool in Fabric without needing external libraries.
- **Automatic Table Detection**: The notebook automatically detects which tables to import, as long as the Synapse Link shortcut points to the root of the ADLS storage account container created by Synapse Link.
- **Soft Deletes**: Records marked as deleted by Synapse Link will be marked as deleted in the Fabric table via the column IsDelete = 1.
- **Error Handling**: If an error occurs for a given folder, the script will abort and retry processing that folder the next time the notebook is run.
- **Logging**: Log messages are outputted to the console and to <incremental_merge_folder>/logs.


<br>

**Disclaimer**: _The Fabric notebook presented here is intended for demonstration purposes. Before deploying it in your environment(s), it is recommended to thoroughly verify and further refine the code to ensure it meets your requirements for correctness, resiliency, performance etc_.

<br>

**Known Issues and Limitations**
<br>
See [issues](https://github.com/arasdk/fabric-code-samples/issues) for an up to date list.

<br>

### Notebook Parameters
The notebook can be parameterized to support execution via Fabric pipelines. This is the recommended approach as the scheduling options on a notebook in Fabric doesn't protect against overlapping executions.
- **lakehouse_name**: The name of the lakehouse where the Synapse Link incremental feed is available as a shortcut.
- **incremental_merge_folder**: The folder in the lakehouse used for logs and watermarks.  (the folder specified will be automatically created if missing)
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
Before the merge script can start loading data into managed tables in a Fabric Lakehouse, some prerequisites are needed to set up this solution: 

- The Dynamics 365 Finance and Operations environment is updated to a version that supports Synapse Link.
- A Storage Account to use with ADLS (Azure Data Lake Storage) gen2 needs to be provisioned in an Azure Subscription.
- A Synapse Link profile must be configured to write incremental files to the ADLS storage account.
- Configure a Fabric Lakehouse, import the notebook and create a shortcut to the data lake under the Files section of the Lakehouse.

<br>

### Dynamics 365 Finance and Operations
Microsoft lists the [supported versions](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-select-fno-data) as part of their Synapse Link documentation, so make sure you check for compatibility with your ERP operations team.

![image](https://github.com/arasdk/fabric-code-samples/assets/145650154/d46c6744-0456-4dc7-9a41-2df017dba921)

The Finance and Operations environment must be [linked](https://learn.microsoft.com/en-us/dynamics365/fin-ops-core/dev-itpro/power-platform/enable-power-platform-integration#enable-during-deploy) with the Power Platform to enable access for Synapse Link.

<br>

### ADLS Gen 2 Storage Account
Incremental files must be written to an [ADLS Gen2 storage account](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-data-lake). You should have owner and storage blob contributor access to the storage account before continuing with the Synapse Link profile setup. 

![image](https://github.com/arasdk/fabric-code-samples/assets/145650154/439f0216-c017-4c66-a812-8f4dfa977bce)

Additional requirements and guidance for setting up the storage account can be found by following the above link.


<br>

### Synapse Link Profile Setup
Start by checking that the [prerequisites](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-select-fno-data#prerequisites) for Synapse Link are fulfilled. Note: If you are on version 10.0.0.39 (PU63) or later of D365FO then row version change tracking should already be automatically enabled for most of the tables and entities you are likely to use.

**Note**: You need system administrator privileges for the Dataverse environment linked to Dynamics 365 Finance and Operations in order to configure Synapse Link.

Once the prerequisites are completed, sign in to [Power Apps](https://make.powerapps.com/) and open "Azure Synapse Link" - remember to select the correct Dataverse environment.
From the Synapse Link app, chose "New Link" and enter the details of the storage account that was provisioned earlier. Leave the "Connect to your Azure Synapse Analytics workspace" option unchecked to configure the profile for incremental CSV files.

When [selecting D365FO tables or entities](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-select-fno-data), you can ignore the options for "Append-only" and "Partition" strategies. Once the profile is saved, "Append-only" will automatically be set to read-only and will have a checkmark next to it. Similarly, "Partition" will be set to "Year."

**Note**: You need to select Advanced -> Show advanced configuration settings -> Enable Incremental Update Folder Structure in order to be able to select D365FO tables and entities.

Once the tables an entities has been selected and the profile has been saved, Synapse Link will start the proces of initializing the feed. This can take anywhere from hours to even days until all tables and entities are sync'ed to the data lake.

<br>

### Configure a Microsoft Fabric Lakehouse
When you have created a Fabric Lakehouse to hold your tables from Synapse Link, you need to add a shortcut to the ADLS storage account.

- Go to the "Files" section of your Lakehouse, right-click and select "New folder". Give the folder the name synapse_link.
- Next, right-click on the synapse_link folder and select "New shortcut".
- From the shortcut wizard navigate to the ADLS storage account and select the container created by Synapse Link for your Dataverse environment. The container should be named dataverse-environment-name-environment uniqe-id. Note down the name given to the shortcut as it will be needed later on.
- Create a Fabric notebook in the same workspace as the Lakehouse and insert the source code from synapse_link_adls_incremental_merge.py.
- Ensure the notebook parameters are configured with the correct values for your Lakehouse.

**Important**: Make sure the Lakehouse is added to your notebook and configured as the default Lakehouse for the notebook. You can verify that the Lakehouse is the default Lakehouse when a "pin" icon is displayed next to the Lakehouse with the notebook opened. If this step is skipped, the script will not be able to take advantage of the Lakehouse default mount point (i.e. "/lakehouse/default/Files/...") for the "Files" folder when running the spark job.

<br>


## Understanding the Incremental Folder Structure and Common Data Model Format
Once the D365FO tables and entities are selected and the profile is saved, Synapse Link begins the process of initializing the incremental folder structure in the data lake. This starts with a full load of all selected tables and entities. Depending on the size and number of items selected, this process can take anywhere from a few hours to several days.
Unlike Export to Data Lake, the Synapse Link incremental updates folder structure for D365FO tables does not support in-place updates. Once records are written to a CSV file by Synapse Link, they are never modified. If a record changes, a new entry is appended to the CSV file. To generate an accurate table, you need to process all incremental folders in the order they were created and apply the changes to the Fabric table.
Note: You can re-export a table from scratch by removing/adding it from the Synapse Link profile.

On the storage account, Synapse Link creates and manages a new storage container exclusively for the incremental update folders structure.
The basic folder structure is pictured below:

![image](https://github.com/arasdk/fabric-code-samples/assets/145650154/f05db364-5269-4841-9e51-de93933c017c)

### Timestamp folders
Synapse Link continuously creates timestamp folders based on the timer interval configured for the profile. Changed, added, or deleted records are always appended to the current folder, which bears the latest timestamp.
At any given time, the name of the current folder is available as a simple text string in the changelog.info file.
All previously processed folder names are written to Changelog/Synapse.log as a chronologically ordered list of strings. This is advantageous because it saves us from performing an expensive list operation on the storage account.
The timestamp folders contains a subfolder for each table where records have been appended.
Each timestamp folder also contains a model.json schema file. For schema evolution, it is practical to store the schema with each batch of files written using that schema. If the schema changes later, the timestamp folders created after the change will accurately reflect the schema applied at the time of writing the records.

### Table Schema & Partitions
The schema and partition information for exported tables and entities are written to timestamp-folder/model.json file.
The overall structure looks like this:

![image](https://github.com/arasdk/fabric-code-samples/assets/145650154/07acd510-0d9c-4eb3-bc12-458095ea7408)

Each table or entity is an element in the entities list. For each list element, there are three key items to note:
- **Name Field**: This contains the table or entity name.
- **Attributes List**: This contains the schema represented as a list of columns, with each column's data type specified with constraints (where applicable).
- **Partitions List**: This includes the name and location of the CSV file. This is based on the "createdon" timestamp of the appended record and will default to "1900" when no such date is available for the table or entity in question

#### Attributes
![image](https://github.com/arasdk/fabric-code-samples/assets/145650154/e08e17a9-3b75-4ddf-bd52-ec2b7f810f25)

The dataType specifier must be mapped to compatible Delta Table data types when generating the schema.
Columns of note here:
- **Id**: The id is used to correlate exported records with the records in the target table
- **SinkCreatedOn**: the datetime when the record was first exported to the data lake by the profile
- **SinkModifiedOn**: the datetime when the record was most recently appended to the data lake
- **sysrowversion**: The SQL row version change tracking version at the time the record was appended. This can be used to compare with the destination record row version during merges, to determine whether to apply the change.
- **IsDelete**: Is set to 1 for deleted records. For Created or updated records the value is NULL. Note: sysrowversion is not updated for deleted records. In fact, most of the columns will be exported to the data lake with NULL value for records that are deleted. Notable exceptions are: Id, SinkCreatedOn, SinkModifiedOn, IsDelete.
<br> 
**Note:** SinkCreatedOn and SinkModifiedOn are tied to the specific Synapse Link profile, and will not correlate to other Synapse Link profiles where the table might be included.

#### Partitions
![image](https://github.com/arasdk/fabric-code-samples/assets/145650154/82ad9fec-4e10-48d1-8bad-fbb06352ad4c)

The partitions list hold an element for each CSV file appended by Synapse Link for the given <timestamp folder>/<table name>/<partition name>.csv
Since the schema for all tables configured in Synapse Link is exported for every timestamp folder created, some tables will have an empty partitions list. This indicates that no records were written for these tables during the time period covered by the timestamp folder. 
The partition list is very useful to determine which tables and CSV files to load without having to resort to performing list operations on the data lake. This is especially useful if we are dealing with a large number of tables.

![image](https://github.com/arasdk/fabric-code-samples/assets/145650154/0d1fa984-8fef-4ea5-9136-e7218b5bfc62)

Files are always appended to the csv file, and the file is named after the year-part of the "createdon" timestamp.
Note: Synapse Link can sometimes subdivide the partitons into smaller files when required. This means records belonging to the partition 2024 can be written to the data lake as:
- Timestamp-folder/mytable/2024.csv
- Timestamp-folder/mytable/2024_001.csv
- Timestamp-folder/mytable/2024_002csv
- Etc.
<br>
