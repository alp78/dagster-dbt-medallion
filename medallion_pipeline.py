# import boto3
# import botocore
# from botocore.config import Config
import pandas as pd
import sqlite3
import os
from pathlib import Path
from typing import cast, Mapping, Any

from dagster import (
    Definitions, 
    AssetExecutionContext, 
    asset, 
    MaterializeResult, 
    MetadataValue,
    AssetsDefinition,
    AssetKey,
)
from dagster_dbt import (
    DbtProject,
    DbtCliResource,
    dbt_assets,
    DagsterDbtTranslator,
)

# --- 1. Define Constants ---

# This finds the user's home directory (e.g., C:\Users\alex)
# and builds the path to your local sources.
# A new user only needs to change the relative part.
USER_HOME = Path.home()
LOCAL_SOURCES_DIR = USER_HOME / "DEV_PY" / "DBT" / "sources"
# ---

# --- S3 Constants
# BUCKET_NAME = ""
# ITEM_KEY = ""
# EVENT_KEY = ""
# ---

# Use absolute path to ensure Python and dbt access the same file
DB_FILE = str(Path(__file__).parent / "medallion.db")
DBT_PROJECT_DIR = Path(__file__).parent / "local_medallion"
DBT_PROFILES_DIR = Path.home() / ".dbt" 
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

# --- 2. Define dbt Resource ---
dbt_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    profiles_dir=os.fspath(DBT_PROFILES_DIR)
)

# Auto-prep manifest - this generates it if missing
dbt_project = DbtProject(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    profiles_dir=os.fspath(DBT_PROFILES_DIR),
    target="dev"
)
dbt_project.prepare_if_dev()

# Verify manifest exists
if not DBT_MANIFEST_PATH.exists():
    raise FileNotFoundError(
        f"dbt manifest not found at {DBT_MANIFEST_PATH}. "
        f"Run 'dbt parse' in {DBT_PROJECT_DIR}"
    )

# --- 3. Define Bronze Layer (Python Assets) ---
@asset(
    group_name="bronze",
    compute_kind="python",
    key=AssetKey(["bronze_item"]),
)
def bronze_item(context: AssetExecutionContext) -> MaterializeResult:
    """Loads item.csv from the local SOURCES_DIR into SQLite."""
    
    # --- S3 Logic (Commented out) ---
    # context.log.info(f"Downloading {ITEM_KEY} from S3...")
    # s3 = boto3.client('s3', config=Config(signature_version=botocore.UNSIGNED))
    # with open("item.csv", "wb") as f:
    #     s3.download_fileobj(BUCKET_NAME, ITEM_KEY, f)
    # ---
    
    # --- New Local Logic ---
    local_item_path = LOCAL_SOURCES_DIR / "item.csv"
    context.log.info(f"Loading {local_item_path} from local disk...")
    # ---

    context.log.info(f"Loading data into SQLite table 'bronze_item'...")
    item_cols = ["adjective", "category", "created_at", "id", "modifier", "name", "price"]
    
    # Read from the local_item_path
    df_item = pd.read_csv(local_item_path, dtype=str, names=item_cols, header=0)
    
    with sqlite3.connect(DB_FILE) as conn:
        df_item.to_sql("bronze_item", conn, if_exists="replace", index=False)
    
    # os.remove("item.csv") # No longer needed
    
    count = len(df_item)
    context.log.info(f"Loaded {count} rows into 'bronze_item'.")
    
    return MaterializeResult(metadata={"row_count": MetadataValue.int(count)})

@asset(
    group_name="bronze",
    compute_kind="python",
    key=AssetKey(["bronze_event"]),
)
def bronze_event(context: AssetExecutionContext) -> MaterializeResult:
    """Loads event.csv from the local SOURCES_DIR into SQLite."""

    # --- S3 Logic (Commented out) ---
    # context.log.info(f"Downloading {EVENT_KEY} from S3...")
    # s3 = boto3.client('s3', config=Config(signature_version=botocore.UNSIGNED))
    # with open("event.csv", "wb") as f:
    #     s3.download_fileobj(BUCKET_NAME, EVENT_KEY, f)
    # ---
    
    # --- New Local Logic ---
    local_event_path = LOCAL_SOURCES_DIR / "event.csv"
    context.log.info(f"Loading {local_event_path} from local disk...")
    # ---

    context.log.info(f"Loading data into SQLite table 'bronze_event'...")
    event_cols = ["event_id", "event_time", "user_id", "event_payload"]

    # Read from the local_event_path
    df_event = pd.read_csv(
        local_event_path, 
        dtype=str, 
        engine='python', 
        names=event_cols, 
        header=0
    )
    
    with sqlite3.connect(DB_FILE) as conn:
        df_event.to_sql("bronze_event", conn, if_exists="replace", index=False)

    # os.remove("event.csv") # No longer needed
    
    count = len(df_event)
    context.log.info(f"Loaded {count} rows into 'bronze_event'.")
    
    return MaterializeResult(metadata={"row_count": MetadataValue.int(count)})

# --- 4. Define dbt Assets (Silver & Gold) ---
# (This section is unchanged and correct)
class LayeredDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        # Simple key using model name
        return AssetKey([dbt_resource_props["name"]])
    
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        # Group by layer for UI organization
        node_name = dbt_resource_props.get('name', '')
        if 'silver' in node_name:
            return "silver"
        if 'gold' in node_name:
            return "gold"
        return "dbt"

# Use string path for manifest
@dbt_assets(
    manifest=str(DBT_MANIFEST_PATH),
    select="silver_item silver_event",  # Explicit model selection
    dagster_dbt_translator=LayeredDbtTranslator(),
)
def silver_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Runs dbt build for silver layer only."""
    yield from dbt.cli(["build", "--select", "silver_item silver_event"], context=context).stream()

@dbt_assets(
    manifest=str(DBT_MANIFEST_PATH),
    select="gold_top_item",  # Explicit model selection
    dagster_dbt_translator=LayeredDbtTranslator(),
)
def gold_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Runs dbt build for gold layer only."""
    yield from dbt.cli(["build", "--select", "gold_top_item"], context=context).stream()

# --- 5. Define All Definitions for Dagster ---
# (This section is unchanged and correct)
defs = Definitions(
    assets=[
        bronze_item, 
        bronze_event, 
        cast(AssetsDefinition, silver_dbt_assets), 
        cast(AssetsDefinition, gold_dbt_assets)
    ],
    resources={
        "dbt": dbt_resource,
    },
)