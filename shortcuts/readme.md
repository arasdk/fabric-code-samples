# Fabric Shortcut Creator

This Python script allows you to automate the process of creating OneLake shortcuts in Microsoft Fabric using the [Fabric REST API](https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-shortcuts). It is suited for bulk-creation of table shortcuts between Fabric workspaces as an alternative for manually creating them using the shortcut Wizard in Fabric.

<br>

## About Shortcuts
[Shortcuts](https://learn.microsoft.com/en-us/fabric/onelake/onelake-shortcuts) are a way to reference tables or files/folders in Fabric without physically copying or moving them between Workspaces. Shortcuts in Microsoft Fabric fundementally works at the OneLake protocol level. The script takes advantage of this fact by leveraging Fabric [mssparkutils](https://learn.microsoft.com/en-us/fabric/data-engineering/microsoft-spark-utilities) to search for delta tables at source URI and compare with (and check for) existing shortcuts at the destination URI.

<br>

## Parameters
The script contains the following parameters:

- **SOURCE_URI**: The source 'abfss' OneLake uri where the shortcut should point to. This should point to a Files or Tables path in a Fabric Workspace and Lakehouse.
- **DEST_URI**: The destination 'abfss' OneLake uri where the shortcut should be created. This should point to a Files or Tables path in a Fabric Workspace and Lakehouse.
- **PATTERN_MATCH**: A collection of string pattern to match the source URI for creating shortcuts. This is only applicable when the source URI points to a /Tables folder in OneLake, and only a subset of tables should be shortcut.

All URI's should be provided using the following form: 
`abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<item-id>/<path>`

<br>


## Getting Started
Until Fabric supports running Python scripts outside of spark clusters, The quickest way to run the script is to create a Fabric notebook and run it using a Spark environment.

1. Create a notebook in Microsoft Fabric
2. Create a cell to hold the notebook parameters (SOURCE_URI, DEST_URI and PATTERN_MATCH)
3. In the lower right hand corner of the cell, mark the cell as a parameter cell. This will allow you to parse in the parameters to the notebook when executing it from another notebook or a data pipeline.
4. copy-paste the remaining source code into a cell created just below the parameter cell
5. Save the notebook with an appropriate name
6. Execute the notebook either directly or from another notebook/pipeline.

<br>

**Example of a parameter cell in a Microsoft Fabric notebook**:
<img width="788" alt="image" src="https://github.com/arasdk/fabric-code-samples/assets/145650154/b34babe0-5b00-4a19-b474-57d7a259db26">

<br>

## Usage Scenarios
The following table presents some usage scenarios with different parameter settings.
The notebook supports a few different scenarios for creating shortcuts for both 'Files' and 'Tables', but is mostly aimed at bulk-operations for 'Tables'.

<br>

| Source URI                                                  | Destination URI                                             | Pattern Match          | Comment                                                                                           |
|-------------------------------------------------------------|-------------------------------------------------------------|------------------------|---------------------------------------------------------------------------------------------------|
| `abfss://workspace1@onelake.dfs.fabric.microsoft.com/lakehouse1/Files/my-folder` | `abfss://workspace2@onelake.dfs.fabric.microsoft.com/lakehouse2/Files` | n/a                | Creates a shortcut in the destination lakehouse: `abfss://workspace2@onelake.dfs.fabric.microsoft.com/lakehouse2/Files/my-folder`             |
| `abfss://workspace1@onelake.dfs.fabric.microsoft.com/lakehouse1/Tables/custtable` | `abfss://workspace2@onelake.dfs.fabric.microsoft.com/lakehouse2/Tables` | ["*"]                  | Creates a shortcut to custtable in the destination lakehouse: `abfss://workspace2@onelake.dfs.fabric.microsoft.com/lakehouse2/Tables/custtable`            |
| `abfss://workspace1@onelake.dfs.fabric.microsoft.com/lakehouse1/Tables` | `abfss://workspace2@onelake.dfs.fabric.microsoft.com/lakehouse2/Tables` | ["cust*"]              	 | Matches all tables at source starting with "cust" and creates shortcuts for them in the destination path.  |
| `abfss://workspace1@onelake.dfs.fabric.microsoft.com/lakehouse1/Tables` | `abfss://workspace2@onelake.dfs.fabric.microsoft.com/lakehouse2/Tables` | ["*sales*"]            | Matches all tables at source containing "sales" in their names and creates shortcuts in the destination path.|
| `abfss://workspace1@onelake.dfs.fabric.microsoft.com/lakehouse1/Tables` | `abfss://workspace2@onelake.dfs.fabric.microsoft.com/lakehouse2/Tables` | ["*"]         | Matches all tables at source and creates shortcuts for them in the destination path. |

<br>

The \<path\> URI segment can contain the following values: 
- **'Files'**: A lakehouse files folder
- **'Files/\<some-path\>'**: A lakehouse sub-folder under 'Files'
- **'Tables'**: A lakehouse 'Tables' folder
- **'Tables/\<some-table-name\>'**: A specific lakehouse table path

<br>

## Explanation

The script performs the following steps:

1. Imports the necessary libraries and modules.
2. Defines helper functions for extracting components from the URI, checking the validity of the URI, and checking if a folder is a delta table.
3. Creates a FabricRestClient object using the default credentials of the executing user.
4. Defines a function to get the last segment of a path.
5. Defines a function to get the details of a shortcut given its URI.
6. Defines a function to get the URIs of delta tables that match the specified patterns.
7. Defines a function to create a shortcut from a source URI to a destination URI.
8. Initializes an empty list to store the created shortcuts.
9. Checks if the source and destination URIs are valid. If not, prints an error message and exits.
10. Removes any trailing slashes from the URIs.
11. If the destination path is not a managed table folder or the source URI is a delta table, creates a file shortcut from the source URI to the destination URI.
12. If the source URI is not a delta table and the destination path is a managed table folder, iterates over the folders (delta tables) in the source URI that match the specified patterns and creates shortcuts to the destination URI.
13. Exits the script and returns the list of created shortcuts as a notebook return value.


<br>

## Limitations

- The script uses the default credentials of the user or service account executing the notebook. It is assumed that the user or service account has the necessary permissions to create shortcuts for the specified Fabric OneLake locations.
- The script does not handle errors that may occur during the creation of shortcuts. It simply prints the error message and continues.
- The script does not handle cases where the destination path already exists. It will skip creating a shortcut in such cases.
- The scipt uses the [sempy](https://learn.microsoft.com/en-us/python/api/semantic-link-sempy/sempy.fabric?view=semantic-link-python) FabricRestClient which is still in it's early stages of development. So the library behavior may change in the future.
- The script doesn't clean up redundant or obsolete shortcuts at the destination Lakehouse that may have been removed at source.

<br>

## License

This script is licensed under the [MIT License](../LICENSE).
