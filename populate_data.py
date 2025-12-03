"""
Script to populate Snowflake tables with Streamlit documentation data.
This runs locally and uploads data to your Snowflake account.
"""

import json
import re
import requests
import pandas as pd
from packaging import version
from snowflake.snowpark import Session

# Connection parameters - reads from .streamlit/secrets.toml format
CONNECTION_PARAMS = {
    "account": "ZSXUBDN-SBC86757",
    "user": "vmakubi",
    "password": "MamaDaniel100!",
    "warehouse": "COMPUTE_WH",
    "database": "ST_ASSISTANT",
    "schema": "PUBLIC",
}


def get_session():
    """Create a Snowpark session."""
    return Session.builder.configs(CONNECTION_PARAMS).create()


def simple_text_splitter(text, chunk_size=1000, chunk_overlap=200):
    """Simple text splitter that breaks text into chunks."""
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        chunks.append(chunk)
        start = end - chunk_overlap
        if start < 0:
            start = 0
        if end >= len(text):
            break
    return chunks


def get_docs_pages_df():
    """Fetch and process Streamlit documentation pages."""
    print("Fetching Streamlit documentation pages...")
    
    PAGE_SEP_RE = re.compile("^---$", flags=re.MULTILINE)
    URL_RE = re.compile("^Source: (.*)$", flags=re.MULTILINE)
    
    url = "https://docs.streamlit.io/llms-full.txt"
    response = requests.get(url)
    response.raise_for_status()
    full_str = response.text
    
    page_strs = PAGE_SEP_RE.split(full_str)
    print(f"Found {len(page_strs)} pages")
    
    page_table_rows = []
    
    for page_str in page_strs:
        url = None
        
        for match in URL_RE.finditer(page_str):
            if match.lastindex == 1:
                url = match[1]
                break
        
        chunks = simple_text_splitter(page_str)
        
        for chunk in chunks:
            page_table_rows.append(
                dict(
                    PAGE_URL=url,
                    PAGE_CHUNK=chunk,
                )
            )
    
    print(f"Created {len(page_table_rows)} page chunks")
    return pd.DataFrame(page_table_rows)


def get_docstrings_df():
    """Fetch and process Streamlit docstrings."""
    print("Fetching Streamlit docstrings...")
    
    url = "https://raw.githubusercontent.com/streamlit/docs/refs/heads/main/python/streamlit.json"
    response = requests.get(url)
    response.raise_for_status()
    docstrings_dict = json.loads(response.text)
    
    # Find latest version
    all_versions = []
    for v_str in docstrings_dict.keys():
        try:
            v = version.parse(v_str)
            all_versions.append(v)
        except version.InvalidVersion:
            continue
    
    latest_version = max(all_versions)
    docstrings_dict["latest"] = docstrings_dict[str(latest_version)]
    print(f"Detected latest Streamlit version as {latest_version}")
    
    docstrings_table_rows = []
    
    for st_version, version_docs in docstrings_dict.items():
        for command_name, command_docstring_obj in version_docs.items():
            # Convert to string if it's a dict/object
            if isinstance(command_docstring_obj, dict):
                chunk_str = json.dumps(command_docstring_obj)
            else:
                chunk_str = str(command_docstring_obj)
            
            chunks = simple_text_splitter(chunk_str, chunk_size=2000, chunk_overlap=200)
            
            for chunk in chunks:
                docstrings_table_rows.append(
                    dict(
                        STREAMLIT_VERSION=st_version,
                        COMMAND_NAME=command_name,
                        DOCSTRING_CHUNK=chunk,
                    )
                )
    
    print(f"Created {len(docstrings_table_rows)} docstring chunks")
    return pd.DataFrame(docstrings_table_rows)


def main():
    print("=" * 60)
    print("Populating Snowflake with Streamlit documentation data")
    print("=" * 60)
    
    # Get the data
    docs_pages_df = get_docs_pages_df()
    docstrings_df = get_docstrings_df()
    
    # Connect to Snowflake
    print("\nConnecting to Snowflake...")
    session = get_session()
    print("Connected!")
    
    # Create database and tables if they don't exist
    print("\nSetting up database and tables...")
    session.sql("CREATE DATABASE IF NOT EXISTS ST_ASSISTANT").collect()
    session.sql("CREATE SCHEMA IF NOT EXISTS ST_ASSISTANT.PUBLIC").collect()
    
    session.sql("""
        CREATE TABLE IF NOT EXISTS ST_ASSISTANT.PUBLIC.STREAMLIT_DOCS_PAGES_CHUNKS (
            PAGE_URL STRING,
            PAGE_CHUNK STRING
        )
    """).collect()
    
    session.sql("""
        CREATE TABLE IF NOT EXISTS ST_ASSISTANT.PUBLIC.STREAMLIT_DOCSTRINGS_CHUNKS (
            STREAMLIT_VERSION STRING,
            COMMAND_NAME STRING,
            DOCSTRING_CHUNK STRING
        )
    """).collect()
    
    # Enable change tracking for Cortex Search
    session.sql("ALTER TABLE ST_ASSISTANT.PUBLIC.STREAMLIT_DOCS_PAGES_CHUNKS SET CHANGE_TRACKING = TRUE").collect()
    session.sql("ALTER TABLE ST_ASSISTANT.PUBLIC.STREAMLIT_DOCSTRINGS_CHUNKS SET CHANGE_TRACKING = TRUE").collect()
    
    # Truncate existing data
    print("Truncating existing data...")
    session.sql("TRUNCATE TABLE ST_ASSISTANT.PUBLIC.STREAMLIT_DOCS_PAGES_CHUNKS").collect()
    session.sql("TRUNCATE TABLE ST_ASSISTANT.PUBLIC.STREAMLIT_DOCSTRINGS_CHUNKS").collect()
    print("Tables ready")
    
    # Upload the data
    print("\nUploading documentation pages...")
    session.write_pandas(
        docs_pages_df,
        database="ST_ASSISTANT",
        schema="PUBLIC",
        table_name="STREAMLIT_DOCS_PAGES_CHUNKS",
        auto_create_table=False,
        overwrite=False,
    )
    print(f"Uploaded {len(docs_pages_df)} page chunks")
    
    print("\nUploading docstrings...")
    session.write_pandas(
        docstrings_df,
        database="ST_ASSISTANT",
        schema="PUBLIC",
        table_name="STREAMLIT_DOCSTRINGS_CHUNKS",
        auto_create_table=False,
        overwrite=False,
    )
    print(f"Uploaded {len(docstrings_df)} docstring chunks")
    
    # Verify
    print("\n" + "=" * 60)
    print("Verification")
    print("=" * 60)
    
    for table in ["STREAMLIT_DOCS_PAGES_CHUNKS", "STREAMLIT_DOCSTRINGS_CHUNKS"]:
        count = session.sql(f"SELECT COUNT(*) FROM ST_ASSISTANT.PUBLIC.{table}").collect()[0][0]
        print(f"{table}: {count} rows")
    
    session.close()
    print("\nDone! You can now run the Streamlit app.")


if __name__ == "__main__":
    main()
