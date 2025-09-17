import requests
import os
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv
from datetime import datetime

def load_env_vars():
    # from scripts.load_secrets import load_secrets_local
    """Load environment variables."""
    load_dotenv("./.env")
    # load_env_secrets_sharepoint()
    # load_env_secrets_sharepoint()
    return {
        "tenant_id": os.getenv("TENANT_ID"),
        "client_id": os.getenv("CLIENT_ID"),
        "client_secret": os.getenv("CLIENT_SECRET"),
        "site_name": os.getenv("SITE_NAME"),
        "directory_path": os.getenv("DIRECTORY_PATH"),
        "base_path": os.getenv("BASE_PATH"),
        "local_directory": os.getenv("LOCAL_DIRECTORY")
    }

def get_access_token(env_vars):
    """Authenticate and obtain an access token."""
    print("Authenticating and obtaining access token...")
    auth_url = f"https://login.microsoftonline.com/{env_vars['tenant_id']}/oauth2/v2.0/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": env_vars['client_id'],
        "client_secret": env_vars['client_secret'],
        "scope": "https://graph.microsoft.com/.default",
    }
    response = requests.post(auth_url, data=payload)
    response.raise_for_status()
    print("Access token obtained successfully.")
    return response.json().get("access_token")

def get_site_id(access_token, site_name):
    """Get the site ID for the specified SharePoint site."""
    print(f"Fetching site ID for site: {site_name}...")
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_name}", headers=headers
    )
    response.raise_for_status()
    site_id = response.json().get("id")
    return site_id

def list_files_in_directory(access_token, site_id, directory_path):
    """List all files in a given directory on SharePoint."""
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{directory_path}:/children"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    files = response.json().get("value", [])
    return files

def download_file(access_token, site_id, folder_path, **read_kwargs):
    """
    Searches a SharePoint folder for the first .csv or Excel file and returns it as a DataFrame.

    Parameters:
        access_token (str): OAuth token
        site_id (str): SharePoint site ID
        folder_path (str): Path to the folder (e.g., 'Shared Documents/myfolder')
        read_kwargs: Additional arguments for pandas read_csv/read_excel

    Returns:
        pd.DataFrame: DataFrame from the first matching file
    """
    headers = {"Authorization": f"Bearer {access_token}"}

    # Step 1: List items in the folder
    files = list_files_in_directory(access_token, site_id, folder_path)
    downloaded_files = []

    # Step 2: Filter for CSV or Excel files
    supported_ext = [".csv", ".xlsx", ".xls"]
    for file in files:
        if file.get("file"):  # Ensure it is a file and not a folder
            name = file.get("name", "")
            ext = os.path.splitext(name)[1].lower()

            if ext in supported_ext:
                # Step 3: Download matching file
                file_path = f"{folder_path}/{name}"
                file_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{file_path}:/content"
                file_resp = requests.get(file_url, headers=headers)
                file_resp.raise_for_status()

                files_bytes = file_resp.content
                file_stream = BytesIO(files_bytes)

                # if ext == ".csv":
                #     df = pd.read_csv(file_stream, **read_kwargs)
                # else:
                #     df = pd.read_excel(file_stream, **read_kwargs)
                downloaded_files.append((file_stream, name))
                # return file_stream, name

    if not downloaded_files:
        raise FileNotFoundError(f"No supported files (.csv, .xlsx, .xls) found in folder: {folder_path}")

    return downloaded_files

def download_directory(access_token, site_id, directory_path, local_directory):
    """Download all files from a directory on SharePoint."""
    print(f"Creating local directory: {local_directory}...")
    try:
        os.makedirs(local_directory, exist_ok=True)  # Create the directory if it doesn't exist
    except OSError as e:
        print(f"Error creating directory {local_directory}: {e}")
        return

    files = list_files_in_directory(access_token, site_id, directory_path)
    for file in files:
        if file.get("file"):  # Ensure it is a file and not a folder
            file_name = file.get("name")
            file_path = f"{directory_path}/{file_name}"
            local_file_path = os.path.join(local_directory, file_name)
            download_file(access_token, site_id, file_path, local_file_path, local_directory)
    print(f"All files in the {directory_path} have been downloaded.")

# function to delete all files in a directory
def delete_files_in_directory(directory):
    """Delete all files in a directory."""
    for file in os.listdir(directory):
        file_path = os.path.join(directory, file)
        try:
            os.remove(file_path)
        except OSError as e:
            pass

def upload_to_directory(df: pd.DataFrame, path: str):
    """
    Convert a DataFrame to an Excel file (in memory) and upload it to the given SharePoint directory path.

    Args:
        df (pd.DataFrame): The dataframe to upload.
        path (str): SharePoint folder path relative to the site's default drive root,
                    e.g. 'Shared Documents/MyFolder'. The folder must already exist.

    Returns:
        dict: The DriveItem JSON returned by Microsoft Graph for the uploaded file.
    """
    # 1) Load env + authenticate
    env_vars = load_env_vars()
    access_token = get_access_token(env_vars)
    site_id = get_site_id(access_token, env_vars["site_name"])

    # 2) Serialize DataFrame -> Excel (in memory)
    output = BytesIO()
    try:
        # Prefer openpyxl for .xlsx
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Sheet1")
    except Exception:
        # Fallback to xlsxwriter if available
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Sheet1")
    output.seek(0)
    data = output.getvalue()
    size = len(data)

    # 3) Build target file name and path
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"upload_{timestamp}.xlsx"
    path = (path or "").strip("/")
    item_path = f"{env_vars['base_path']}/{path}/{filename}" if path else filename

    # 4) Upload (simple for ≤4MB, session for larger)
    if size <= 4 * 1024 * 1024:  # ≤ 4MB -> simple upload
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{item_path}:/content"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }
        resp = requests.put(url, headers=headers, data=data)
        resp.raise_for_status()
        return resp.json()

    # >4MB -> create an upload session (chunked upload)
    session_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{item_path}:/createUploadSession"
    session_headers = {"Authorization": f"Bearer {access_token}"}
    session_body = {
        "item": {
            "@microsoft.graph.conflictBehavior": "replace",
            "name": filename,
        }
    }
    session_resp = requests.post(session_url, headers=session_headers, json=session_body)
    session_resp.raise_for_status()
    upload_url = session_resp.json().get("uploadUrl")

    chunk_size = 5 * 1024 * 1024  # 5 MB
    start = 0
    last_resp = None

    while start < size:
        end = min(start + chunk_size, size) - 1
        chunk = data[start : end + 1]
        headers = {
            "Content-Length": str(end - start + 1),
            "Content-Range": f"bytes {start}-{end}/{size}",
        }
        # Note: uploadUrl already contains auth; do not add Authorization header
        put_resp = requests.put(upload_url, headers=headers, data=chunk)
        put_resp.raise_for_status()
        last_resp = put_resp
        start = end + 1

    # When the last chunk is uploaded, Graph returns the DriveItem
    return last_resp.json()
