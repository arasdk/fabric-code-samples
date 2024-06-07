# coding: utf-8

# In[1]:


# URI's should be in the form: abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<item-id>/<path>"
SOURCE_URI = "abfss://<workspace id>@onelake.dfs.fabric.microsoft.com/<item id>/<path>"
DEST_URI = "abfss://<workspace id>@onelake.dfs.fabric.microsoft.com/<item id>/<path>"

# Provide an array of search wildcards or just "*" to shortcut all tables
# A list of specific table names will also be matches specifically such as ["custtable", "salestable", "salesline"]
#
# Wildcard Syntax:
# *      Matches everything
# ?      Matches any single character
# [seq]  Matches any character in seq
# [!seq] Matches any character not in seq
#
# Examples:
# "cust*" # Matches any table beginning with "cust"
# "*cust*" # Matches any table containing the string "cust"
# "custtabl?" Matches "custtable" but not "custtables"
# "custtable_[1-2]" Matches "custtable_1", "custtable_2" but not "custtable_3"
# "custtable_[!1-2]" Matches "custtable_3", "custtable_4" (etc.), but not "custtable_1" or "custtable_2"
PATTERN_MATCH = ["*"]


# In[2]:


import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException
import json
import fnmatch
import re
import os


# https://learn.microsoft.com/en-us/python/api/semantic-link-sempy/sempy.fabric?view=semantic-link-python
# FabricRestClient uses the default credentials of the executing user
client = fabric.FabricRestClient()


# Extract workspace_id, item_id and path from a onelake URI
def extract_onelake_https_uri_components(uri):
    # Define a regular expression to match any string between slashes and capture the final path element(s) without the leading slash
    pattern = re.compile(r"abfss://([^@]+)@[^/]+/([^/]+)/(.*)")
    match = pattern.search(uri)
    if match:
        workspace_id, item_id, path = match.groups()
        return workspace_id, item_id, path
    else:
        return None, None, None


def is_valid_onelake_uri(uri: str) -> bool:
    workspace_id, item_id, path = extract_onelake_https_uri_components(uri)
    if not "abfss://" in uri or workspace_id is None or item_id is None or path is None:
        return False

    return True


def get_last_path_segment(uri: str):
    path = uri.split("/")  # Split the entire URI by '/'
    return path[-1] if path else None


def is_delta_table(uri: str):
    delta_log_path = os.path.join(uri, "_delta_log")
    return mssparkutils.fs.exists(delta_log_path)


def get_onelake_shorcut(workspace_id: str, item_id: str, path: str, name: str):
    shortcut_uri = (
        f"v1/workspaces/{workspace_id}/items/{item_id}/shortcuts/{path}/{name}"
    )
    result = client.get(shortcut_uri).json()
    return result


def get_matching_delta_tables_uris(uri: str, patterns: []) -> []:

    # Use a set to avoid duplicates
    matched_uris = set()
    files = mssparkutils.fs.ls(uri)
    folders = [item for item in files if item.isDir]

    # Add wildcard to pattern to match everything if empty
    if patterns is None or len(patterns) == 0:
        patterns = ["*"]

    # Iterate over each pattern and filter files based on their path property
    for pattern in patterns:
        matched_uris.update(
            folder.path
            for folder in folders
            if is_delta_table(folder.path) and fnmatch.fnmatch(folder.name, pattern)
        )

    return matched_uris


def create_onelake_shorcut(source_uri: str, dest_uri: str):

    src_workspace_id, src_item_id, src_path = extract_onelake_https_uri_components(
        source_uri
    )

    dest_workspace_id, dest_item_id, dest_path = extract_onelake_https_uri_components(
        dest_uri
    )

    name = get_last_path_segment(source_uri)
    dest_uri_joined = os.path.join(dest_uri, name)

    # If the destination path already exists, return without creating shortcut
    if mssparkutils.fs.exists(dest_uri_joined):
        print(f"Destination already exists: {dest_uri_joined}")
        return None

    request_body = {
        "name": name,
        "path": dest_path,
        "target": {
            "oneLake": {
                "itemId": src_item_id,
                "path": src_path,
                "workspaceId": src_workspace_id,
            }
        },
    }

    shortcut_uri = f"v1/workspaces/{dest_workspace_id}/items/{dest_item_id}/shortcuts"
    print(f"Creating shortcut: {shortcut_uri}/{name}..")
    try:
        client.post(shortcut_uri, json=request_body)
    except FabricHTTPException as e:
        print(e)
        return None

    return get_onelake_shorcut(dest_workspace_id, dest_item_id, dest_path, name)


# Collect created shortcuts
result = []

# If either URI's are invalid, just return
if not is_valid_onelake_uri(SOURCE_URI) or not is_valid_onelake_uri(DEST_URI):
    print(
        "invalid URI's provided. URI's should be in the form: abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<item-id>/<path>"
    )
else:
    # Remove any trailing '/' from uri's
    source_uri_addr = SOURCE_URI.rstrip("/")
    dest_uri_addr = DEST_URI.rstrip("/")

    dest_workspace_id, dest_item_id, dest_path = extract_onelake_https_uri_components(
        dest_uri_addr
    )

    # If we are not shortcutting to a managed table folder or
    # the source uri is a delta table, just shortcut it 1-1.
    if not dest_path.startswith("Tables") or is_delta_table(source_uri_addr):
        shortcut = create_onelake_shorcut(source_uri_addr, dest_uri_addr)
        if shortcut is not None:
            result.append(shortcut)
    else:
        # If source is not a delta table, and destination is managed table folder:
        # Iterate over source folders and create table shortcuts @ destination
        for delta_table_uri in get_matching_delta_tables_uris(
            source_uri_addr, PATTERN_MATCH
        ):
            shortcut = create_onelake_shorcut(delta_table_uri, dest_uri_addr)
            if shortcut is not None:
                result.append(shortcut)

# Set return value with created shortcuts JSON objects
mssparkutils.notebook.exit(result)
