# synapse_link_adls_incremental_merge

## Overview
This notebook performs incremental merges of CSV files from a Synapse Link feed in a Data Lakehouse environment using Microsoft Fabric and PySpark. The process handles schema evolution, manages parallel merges, and ensures only the latest records are kept.

- The notebook is fully self-contained and needs only the configuration provided by the parameters.
- It can be executed by a Fabric Pipeline or scheduled directly, although the Pipeline option might be better to avoid stacking multiple notebook executions.
- The notebook will process change folders chronologically until it reaches the current folder.
- It will track the last folder processed by placing a watermark file specified via the "incremental_merge_folder" parameter. The watermark file contains a text string equal to the last processed folder name.
- It can run on any spark starter pool in Fabric and doesn't need external libraries installed.
- It will automatically detect which tables to import as long as the synapse link shortcut is pointing at the root of the ADLS storage account container created by Synapse Link.
- The notebook uses soft-deletes, which means that records marked as deleted by Synapse Link will be marked as deleted in the Fabric table via the column IsDelete = 1.
- If an error occurs for a given folder the script will abort and retry the folder the next time the notebook is run
- Log messages are outputted to console and <incremental_merge_folder>/logs

##Lakehouse setup
In order for the notebook to work, the Fabric Lakehouse containing the synapse link files shortcut should be added and pinned as the default Lakehouse of the notebook.

<br>

## Notebook Parameters
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

## Spark configuration Settings
These are the spark settings used by the script:
```
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)
spark.conf.set("spark.ms.autotune.enabled", "true")
```

