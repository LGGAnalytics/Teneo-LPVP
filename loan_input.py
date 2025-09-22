import pandas as pd
import logging
import sys
import os
from io import BytesIO
from datetime import datetime

# SharePoint ETL functions import
from tools.sharepoint import load_env_vars, get_access_token, get_site_id, download_file

# Custom modules
from input_data.datatape_segmentation import process_loans_dataframe_segmentation
from input_data.index_rate_calculation import process_floating_calculations
from input_data.fixed_rate_calculation import process_fixed_calculations
from input_data.combined_risk import assign_combined_risk_rates
from input_data.fixed_dfs import enrich_loans_with_fixed_assumptions_parallel
from input_data.assumption_tables import load_assumptions_excel_to_dict_from_stream
from calculations import manage_calculations

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# -------------------------------
# SharePoint Integration Functions
# -------------------------------
def download_files_from_sharepoint(folder_path):
    """Download files from specified SharePoint folder with metadata"""
    try:
        logging.info(f"Connecting to SharePoint and downloading files from: {folder_path}")
        
        # Get SharePoint connection
        env_vars = load_env_vars()
        access_token = get_access_token(env_vars)
        site_id = get_site_id(access_token, env_vars['site_name'])
        
        # Build full folder path
        full_folder_path = f"{env_vars['base_path']}/{folder_path}"
        
        # Get folder items with metadata using Graph API
        import requests
        
        folder_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{full_folder_path}:/children"
        headers = {"Authorization": f"Bearer {access_token}"}
        
        response = requests.get(folder_url, headers=headers)
        response.raise_for_status()
        items = response.json().get('value', [])
        
        # Filter Excel files and get their metadata
        excel_files_info = []
        for item in items:
            if item['name'].lower().endswith(('.xlsx', '.xls')):
                excel_files_info.append({
                    'name': item['name'],
                    'id': item['id'],
                    'last_modified': item['lastModifiedDateTime'],
                    'size': item['size']
                })
        
        logging.info(f"Found {len(excel_files_info)} Excel files in {folder_path}")
        for file_info in excel_files_info:
            logging.info(f"  - {file_info['name']} (Modified: {file_info['last_modified']})")
        
        # Download all Excel files
        downloaded_files = []
        for file_info in excel_files_info:
            file_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_info['id']}/content"
            file_response = requests.get(file_url, headers=headers)
            file_response.raise_for_status()
            
            file_stream = BytesIO(file_response.content)
            downloaded_files.append((file_stream, file_info['name'], file_info['last_modified']))
        
        logging.info(f"Downloaded {len(downloaded_files)} Excel files with metadata")
        return downloaded_files
        
    except Exception as e:
        logging.error(f"Error downloading files from SharePoint: {str(e)}")
        return []

def find_latest_excel_file(downloaded_files_with_metadata, keywords=None):
    """Find the most recently modified Excel file, optionally filtering by keywords"""
    if not downloaded_files_with_metadata:
        return None, None
    
    # Filter by keywords if provided
    if keywords:
        filtered_files = []
        for file_stream, filename, modified_date in downloaded_files_with_metadata:
            filename_lower = filename.lower()
            if any(keyword.lower() in filename_lower for keyword in keywords):
                filtered_files.append((file_stream, filename, modified_date))
        
        if filtered_files:
            downloaded_files_with_metadata = filtered_files
            logging.info(f"Filtered to {len(filtered_files)} files matching keywords: {keywords}")
        else:
            logging.warning(f"No files found matching keywords: {keywords}, using all Excel files")
    
    # Sort by modification date (most recent first)
    sorted_files = sorted(downloaded_files_with_metadata, 
                         key=lambda x: x[2], 
                         reverse=True)
    
    latest_file = sorted_files[0]
    file_stream, filename, modified_date = latest_file
    
    logging.info(f"Selected latest file: {filename} (Modified: {modified_date})")
    
    return file_stream, filename

def load_loans_excel_from_stream(file_stream):
    """Load loans dataframe directly from BytesIO stream"""
    try:
        file_stream.seek(0)
        
        # Check what sheets are available
        excel_file = pd.ExcelFile(file_stream)
        available_sheets = excel_file.sheet_names
        logging.info(f"Available sheets in loans Excel: {available_sheets}")
        
        # Try different possible sheet names
        possible_sheet_names = ['Loans', 'loans', 'Loan', 'loan', 'Data', 'data', 'Sheet1']
        
        target_sheet = None
        for sheet in possible_sheet_names:
            if sheet in available_sheets:
                target_sheet = sheet
                logging.info(f"Found target sheet: {target_sheet}")
                break
        
        # If no common sheet name found, use the first available sheet
        if target_sheet is None:
            target_sheet = available_sheets[0]
            logging.info(f"No standard sheet found, using first available sheet: {target_sheet}")
        
        df = pd.read_excel(file_stream, sheet_name=target_sheet, header=0)
        logging.info(f"Successfully loaded {len(df)} rows from sheet '{target_sheet}'")
        logging.info(f"Columns found: {list(df.columns)}")
        
        return df
        
    except Exception as e:
        logging.error(f"Error loading loans DataFrame from stream: {str(e)}")
        return None

def load_assumptions_excel_from_stream(file_stream):
    """Load assumptions Excel directly from BytesIO stream"""
    try:
        logging.info("Loading assumptions Excel from BytesIO stream")
        
        sheet_tables_dict = {
            "Assumption_Loans": ["Cost of Risk - Loan with Guarantee", "Prepayment Risk - Loan with Guarantee"],
            "Index_Analysis": ["Index Type"]
        }

        table_alias_dict = {
            "Assumption_Loans": ["Cost_Risk", "Prepayment_Risk"],
            "Index_Analysis": ["Index_Type"]
        }

        # Use the existing function from assumption_tables.py
        file_stream.seek(0)
        assumptions_raw = load_assumptions_excel_to_dict_from_stream(file_stream, sheet_tables_dict)

        # rename tables using alias
        assumptions_renamed = {}
        for sheet_name, tables in assumptions_raw.items():
            aliases = table_alias_dict.get(sheet_name, list(tables.keys()))
            assumptions_renamed[sheet_name] = {alias: tables[orig] for alias, orig in zip(aliases, tables.keys())}

        logging.info("Assumptions Excel loaded successfully from stream")
        return assumptions_renamed
        
    except Exception as e:
        logging.error(f"Error loading assumptions from stream: {str(e)}")
        return None

def get_excel_streams_from_sharepoint():
    """Download and return BytesIO streams directly from SharePoint Phase 1 and Phase 2"""
    
    # Download from Phase 1 (Loans data) - get latest file
    logging.info("Downloading loans data from Phase 1...")
    phase1_files = download_files_from_sharepoint("Phase 1")
    
    # Find latest loans Excel file (with optional keyword filtering)
    loans_stream, loans_filename = find_latest_excel_file(
        phase1_files, 
        keywords=['data', 'datatape', 'simple', 'loan', 'example']  # Optional filtering
    )
    
    if not loans_stream:
        logging.error("No loans Excel file found in Phase 1")
        return None, None, None, None
    
    # Download from Phase 2 (Assumptions data) - get latest file
    logging.info("Downloading assumptions data from Phase 2...")
    phase2_files = download_files_from_sharepoint("Phase 2")
    
    # Find latest assumptions Excel file (with optional keyword filtering)
    assumptions_stream, assumptions_filename = find_latest_excel_file(
        phase2_files,
        keywords=['assumption', 'template', 'complex']  # Optional filtering
    )
    
    if not assumptions_stream:
        logging.error("No assumptions Excel file found in Phase 2")
        return None, None, None, None
    
    logging.info(f"Successfully prepared latest streams:")
    logging.info(f"  Loans: {loans_filename} (latest from Phase 1)")
    logging.info(f"  Assumptions: {assumptions_filename} (latest from Phase 2)")
    
    return loans_stream, assumptions_stream, loans_filename, assumptions_filename

def main_processing_pipeline_from_streams(loans_stream, assumptions_stream, loans_filename):
    """Modified main processing pipeline to work with BytesIO streams"""
    logging.info("Starting main processing pipeline from streams")

    # Load loans from stream
    loans_df = load_loans_excel_from_stream(loans_stream)
    if loans_df is None:
        logging.error("Failed to load loans data from stream")
        return None, None, None

    # Load assumptions from stream
    assumptions_dicts = load_assumptions_excel_from_stream(assumptions_stream)
    if not assumptions_dicts:
        logging.error("Failed to load assumptions data from stream")
        return None, None, None

    # Debug: Check loaded assumptions data
    logging.info("=== ASSUMPTIONS DEBUG ===")
    for sheet_name, tables in assumptions_dicts.items():
        logging.info(f"Sheet '{sheet_name}':")
        for table_name, table_data in tables.items():
            if isinstance(table_data, dict):
                logging.info(f"  Table '{table_name}': {len(table_data)} loan types")
                if table_data:
                    sample_loan_type = next(iter(table_data.keys()))
                    sample_dates = list(table_data[sample_loan_type].keys()) if table_data[sample_loan_type] else []
                    logging.info(f"    Sample loan type: '{sample_loan_type}'")
                    logging.info(f"    Sample dates: {sample_dates[:5]}")  # First 5 dates
                else:
                    logging.warning(f"    Table '{table_name}' is EMPTY!")
            else:
                logging.warning(f"  Table '{table_name}': Unexpected data type {type(table_data)}")
    logging.info("=== END ASSUMPTIONS DEBUG ===")

    # For segmentation, we need to pass the filename (not full path)
    try:
        # Segment loans
        logging.info("Segmenting loans dataframe...")
        segmented_results = process_loans_dataframe_segmentation(loans_df, loans_filename)

        # Process floating loans
        floating_results = process_floating_loans(segmented_results, assumptions_dicts["Index_Analysis"]["Index_Type"])

        # Process fixed loans
        fixed_results = process_fixed_loans(segmented_results)

        # Combine floating + fixed
        combined_loans = combine_floating_fixed(floating_results, fixed_results)

        # Assign combined risk rates
        logging.info("Assigning combined risk rates...")
        combined_with_risks = assign_combined_risk_rates(
            combined_loans,
            assumptions_dicts["Assumption_Loans"]["Cost_Risk"],
            assumptions_dicts["Assumption_Loans"]["Prepayment_Risk"],
        )

        # For the fixed assumptions enrichment, we might need to create a temporary file
        # since that function might expect a file path
        logging.info("Enriching loans with fixed assumptions...")
        try:
            # Create temporary file just for this function if it needs file path
            import tempfile
            with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as temp_file:
                assumptions_stream.seek(0)
                temp_file.write(assumptions_stream.read())
                temp_assumptions_path = temp_file.name
            
            combined_with_fixed = enrich_loans_with_fixed_assumptions_parallel(
                combined_with_risks, temp_assumptions_path, debug_percentage=True, fix_percentage=True
            )
            
            # Clean up temporary file
            os.unlink(temp_assumptions_path)
            
            logging.info("Fixed assumptions enrichment completed")
        except Exception as e:
            logging.error(f"enrich_loans_with_fixed_assumptions_parallel failed: {e}", exc_info=True)
            combined_with_fixed = combined_with_risks  # Use without fixed enrichment

        logging.info("Main processing pipeline completed successfully")
        return combined_with_fixed, assumptions_dicts, segmented_results
        
    except Exception as e:
        logging.error(f"Error in main processing pipeline: {str(e)}", exc_info=True)
        return None, None, None

def save_results_to_phase3(combined_with_fixed, loans_filename):
    """Save processing results to Phase 3 as Excel"""
    try:
        logging.info("Saving processing results to Phase 3...")
        
        # Create output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = loans_filename.replace('.xlsx', '').replace('.xls', '')
        output_filename = f"Processed_Loans_{base_name}_{timestamp}.xlsx"
        
        # Convert results to Excel in BytesIO
        output_buffer = BytesIO()
        with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
            # Write each loan type to separate sheets
            for type_key, df in combined_with_fixed.items():
                if not df.empty:
                    sheet_name = f"Type_{type_key}"
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    logging.info(f"Added sheet '{sheet_name}' with {len(df)} rows")
                else:
                    logging.info(f"Skipping empty dataset for {type_key}")
        
        output_buffer.seek(0)
        
        # Get SharePoint connection
        env_vars = load_env_vars()
        access_token = get_access_token(env_vars)
        site_id = get_site_id(access_token, env_vars['site_name'])
        
        # Build Phase 3 path
        phase3_path = f"{env_vars['base_path']}/Phase 3/{output_filename}"
        
        # Upload to SharePoint Phase 3
        import requests
        
        # Get file size for upload method decision
        file_data = output_buffer.getvalue()
        file_size = len(file_data)
        
        if file_size <= 4 * 1024 * 1024:  # <= 4MB -> simple upload
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{phase3_path}:/content"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            }
            resp = requests.put(url, headers=headers, data=file_data)
            resp.raise_for_status()
            result = resp.json()
        else:
            # Large file upload with chunked upload
            session_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{phase3_path}:/createUploadSession"
            session_headers = {"Authorization": f"Bearer {access_token}"}
            session_body = {
                "item": {
                    "@microsoft.graph.conflictBehavior": "replace",
                    "name": output_filename,
                }
            }
            session_resp = requests.post(session_url, headers=session_headers, json=session_body)
            session_resp.raise_for_status()
            upload_url = session_resp.json().get("uploadUrl")

            # Upload in chunks
            chunk_size = 5 * 1024 * 1024  # 5 MB
            start = 0
            last_resp = None

            while start < file_size:
                end = min(start + chunk_size, file_size) - 1
                chunk = file_data[start : end + 1]
                headers = {
                    "Content-Length": str(end - start + 1),
                    "Content-Range": f"bytes {start}-{end}/{file_size}",
                }
                put_resp = requests.put(upload_url, headers=headers, data=chunk)
                put_resp.raise_for_status()
                last_resp = put_resp
                start = end + 1
            
            result = last_resp.json()
        
        logging.info(f"SUCCESS: Results saved to Phase 3 as '{output_filename}'")
        logging.info(f"File size: {file_size / (1024*1024):.2f} MB")
        logging.info(f"SharePoint location: {phase3_path}")
        
        return result
        
    except Exception as e:
        logging.error(f"Error saving results to Phase 3: {str(e)}")
        return None

# -------------------------------
# Original Functions (unchanged)
# -------------------------------
def process_fixed_loans(segmented_results):
    logging.info("Processing fixed loans...")
    fixed_results = {}
    fixed_datasets = segmented_results["performing_loans"]["original"]["fixed"]

    for calc_type in ["nf1", "nf2", "nf3", "nf4"]:
        type_number = calc_type[-1]
        type_fixed_data = fixed_datasets[calc_type]
        if not type_fixed_data.empty:
            df_with_rates = process_fixed_calculations({f"type_{type_number}": type_fixed_data})[f"type_{type_number}"]
            fixed_results[f"type_{type_number}"] = df_with_rates
        else:
            fixed_results[f"type_{type_number}"] = pd.DataFrame()
    
    return fixed_results

def process_floating_loans(segmented_results, assumptions_dict):
    floating_results = {}
    floating_original = segmented_results["performing_loans"]["original"]["floating"]
    for i in range(1, 5):
        f_key = f"f{i}"
        type_floating_data = floating_original[f_key]
        if not type_floating_data.empty:
            floating_results[f"type_{i}"] = process_floating_calculations(
                floating_df=type_floating_data,
                assumptions_dict=assumptions_dict
            )
        else:
            floating_results[f"type_{i}"] = pd.DataFrame()
    return floating_results

def combine_floating_fixed(floating_results, fixed_results):
    logging.info("Combining floating and fixed loans...")
    combined_types = {}

    for i in range(1, 5):
        type_key = f"type_{i}"
        floating_df = floating_results.get(type_key, pd.DataFrame()).copy()
        fixed_df = fixed_results.get(type_key, pd.DataFrame()).copy()

        if not floating_df.empty:
            floating_df['interest_rate_type'] = 'floating'
        if not fixed_df.empty:
            fixed_df['interest_rate_type'] = 'fixed'

        combined_df = pd.concat([floating_df, fixed_df], ignore_index=True)
        combined_types[type_key] = combined_df

    return combined_types

def log_portfolio_summary(loans_df: pd.DataFrame):
    logging.info("Loan portfolio summary BEFORE processing:")

    summary_tables = []

    if "Type of Loan" in loans_df.columns:
        type_summary = loans_df["Type of Loan"].value_counts().reset_index()
        type_summary.columns = ["Type of Loan", "Count"]
        summary_tables.append(("Type of Loan", type_summary))

    if "Loan Status" in loans_df.columns:
        status_summary = loans_df["Loan Status"].value_counts().reset_index()
        status_summary.columns = ["Loan Status", "Count"]
        summary_tables.append(("Loan Status (PL/NPL)", status_summary))

    if "Interest Rate Type" in loans_df.columns:
        rate_summary = loans_df["Interest Rate Type"].value_counts().reset_index()
        rate_summary.columns = ["Interest Rate Type", "Count"]
        summary_tables.append(("Interest Rate Type (Floating/Fixed)", rate_summary))

    for title, df in summary_tables:
        logging.info(f"\n{title}:\n{df.to_string(index=False)}")

# -------------------------------
# Main execution with SharePoint integration
# -------------------------------
if __name__ == "__main__":
    try:
        logging.info("Starting main processing with SharePoint integration (BytesIO)")
        logging.info("=" * 60)
        logging.info("Configuration:")
        logging.info("- Loans data source: SharePoint Phase 1 (latest file)")
        logging.info("- Assumptions data source: SharePoint Phase 2 (latest file)")
        logging.info("- Processing: Direct from BytesIO streams (no local files)")
        logging.info("=" * 60)
        
        # Download Excel streams from SharePoint
        result = get_excel_streams_from_sharepoint()
        
        if not result or len(result) != 4:
            logging.error("Failed to download required Excel files from SharePoint")
            sys.exit(1)
        
        loans_stream, assumptions_stream, loans_filename, assumptions_filename = result
        
        # Load raw loans for portfolio summary
        loans_df = load_loans_excel_from_stream(loans_stream)
        if loans_df is not None:
            log_portfolio_summary(loans_df)
        else:
            logging.error("Failed to load loans data for portfolio summary")
            sys.exit(1)

        # Run main pipeline with streams
        combined_with_fixed, assumptions_dicts, segmented_results = main_processing_pipeline_from_streams(
            loans_stream, assumptions_stream, loans_filename
        )

        if combined_with_fixed is not None:
            logging.info("Final processing completed successfully! Loans enriched with risk profiles and fixed assumptions.")
            
            logging.info("Sending results to calculations.py manage_calculations...")
            calculations_result = manage_calculations(combined_with_fixed)

            # Save results to Phase 3
            save_result = save_results_to_phase3(combined_with_fixed, loans_filename)
            
            if save_result:
                logging.info("=" * 60)
                logging.info("PROCESSING AND SAVE COMPLETED SUCCESSFULLY!")
                logging.info("=" * 60)
                logging.info("Summary:")
                logging.info(f"- Input: {loans_filename} (Phase 1)")
                logging.info(f"- Assumptions: {assumptions_filename} (Phase 2)")
                logging.info(f"- Output: Processed results saved to Phase 3")
                logging.info("=" * 60)
            else:
                logging.warning("Processing completed but failed to save to Phase 3")
                logging.info("=" * 60)
                logging.info("PROCESSING COMPLETED (SAVE FAILED)")
                logging.info("=" * 60)
        else:
            logging.error("Processing pipeline failed")
            sys.exit(1)

    except Exception as e:
        logging.error(f"Processing failed: {e}", exc_info=True)
        sys.exit(1)