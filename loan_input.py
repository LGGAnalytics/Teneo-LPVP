import pandas as pd
import logging
import sys
import os

# Custom modules
from input_data.datatape_segmentation import process_loans_dataframe_segmentation
from input_data.assumption_tables import load_assumptions_excel_to_dict
from input_data.index_rate_calculation import process_floating_calculations
from input_data.fixed_rate_calculation import process_fixed_calculations
from input_data.combined_risk import assign_combined_risk_rates
from input_data.fixed_dfs import enrich_loans_with_fixed_assumptions_parallel
from input_data.load_loans import load_loans_excel

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def load_assumptions_excel(excel_file_path):
    logging.info(f"Loading assumptions Excel from: {excel_file_path}")

    sheet_tables_dict = {
        "Assumption_Loans": ["Cost of Risk - Loan with Guarantee", "Prepayment Risk - Loan with Guarantee"],
        "Index_Analysis": ["Index Type"]
    }

    table_alias_dict = {
        "Assumption_Loans": ["Cost_Risk", "Prepayment_Risk"],
        "Index_Analysis": ["Index_Type"]
    }

    assumptions_raw = load_assumptions_excel_to_dict(excel_file_path, sheet_tables_dict)

    # rename tables using alias
    assumptions_renamed = {}
    for sheet_name, tables in assumptions_raw.items():
        aliases = table_alias_dict.get(sheet_name, list(tables.keys()))
        assumptions_renamed[sheet_name] = {alias: tables[orig] for alias, orig in zip(aliases, tables.keys())}

    logging.info("Assumptions Excel loaded successfully")
    return assumptions_renamed

# -------------------------------
# Process fixed loans
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

# -------------------------------
# Process floating loans
# -------------------------------
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

# -------------------------------
# Combine floating + fixed
# -------------------------------
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

# -------------------------------
# Main processing pipeline
# -------------------------------
def main_processing_pipeline(loans_excel_path, assumptions_excel_path):
    logging.info("Starting main processing pipeline")

    # Load loans
    loans_df = load_loans_excel(loans_excel_path)

    # Load assumptions
    assumptions_dicts = load_assumptions_excel(assumptions_excel_path)

    # Segment loans
    logging.info("Segmenting loans dataframe...")
    segmented_results = process_loans_dataframe_segmentation(loans_df, loans_excel_path)

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

    # Enrich with fixed assumptions
    logging.info("Enriching loans with fixed assumptions...")
    try:
        combined_with_fixed = enrich_loans_with_fixed_assumptions_parallel(
            combined_with_risks, assumptions_excel_path, debug_percentage=True, fix_percentage=True
        )
        logging.info("Fixed assumptions enrichment completed")
    except Exception as e:
        logging.error(f"enrich_loans_with_fixed_assumptions_parallel failed: {e}", exc_info=True)

    logging.info("Main processing pipeline completed successfully")
    return combined_with_fixed, assumptions_dicts, segmented_results

# -------------------------------
# Utility: Portfolio summary
# -------------------------------
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
# Main execution
# -------------------------------
if __name__ == "__main__":
    LOANS_EXCEL_PATH = "Example Datatape simple.xlsx"
    ASSUMPTIONS_EXCEL_PATH = "Assumption_Summary_Template_complex.xlsx"

    if not os.path.exists(LOANS_EXCEL_PATH) or not os.path.exists(ASSUMPTIONS_EXCEL_PATH):
        logging.error("Excel files not found. Check paths.")
        sys.exit(1)

    try:
        # Load raw loans for portfolio summary
        loans_df = load_loans_excel(LOANS_EXCEL_PATH)
        log_portfolio_summary(loans_df)

        # Run main pipeline
        combined_with_fixed, assumptions_dicts, segmented_results = main_processing_pipeline(
            LOANS_EXCEL_PATH, ASSUMPTIONS_EXCEL_PATH   
        )

        logging.info("Final processing completed successfully! Loans enriched with risk profiles and fixed assumptions.")

    except Exception as e:
        logging.error(f"Processing failed: {e}", exc_info=True)
        sys.exit(1)