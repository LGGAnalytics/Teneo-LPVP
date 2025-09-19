import pandas as pd
import logging
import json
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp

def assign_combined_risk_rates(combined_loans, cost_risk_df, prepayment_risk_df):
    """
    OPTIMIZED VERSION - Assigns combined risk rates with performance improvements.
    
    Performance optimizations:
    - Vectorized operations where possible
    - Pre-computed risk dictionaries
    - Batch processing
    - Parallel processing for large datasets
    
    Args:
        combined_loans (dict): Dictionary of combined loans by type
        cost_risk_df (pd.DataFrame or dict): Cost of Risk assumptions
        prepayment_risk_df (pd.DataFrame or dict): Prepayment Risk assumptions
    
    Returns:
        dict: Same structure with risk_rates column
    """
    logger = logging.getLogger(__name__)
    logger.info("🚀 Starting OPTIMIZED risk rate assignment...")

    # 0️⃣ Convert dicts to DataFrames if necessary
    if isinstance(cost_risk_df, dict):
        cost_risk_df = pd.DataFrame(cost_risk_df).T.reset_index().rename(columns={"index":"Type of Loan"})
    if isinstance(prepayment_risk_df, dict):
        prepayment_risk_df = pd.DataFrame(prepayment_risk_df).T.reset_index().rename(columns={"index":"Type of Loan"})

    # 1️⃣ OPTIMIZATION: Pre-process and cache risk data
    logger.info("⚡ Pre-processing risk data...")
    risk_lookup = _create_optimized_risk_lookup(cost_risk_df, prepayment_risk_df)

    result = {}

    # 2️⃣ Process each loan type
    for type_key, loans_df in combined_loans.items():
        if loans_df is None or loans_df.empty:
            result[type_key] = pd.DataFrame()
            continue

        logger.info(f"⚡ Processing {type_key} with {len(loans_df)} loans...")
        
        # 3️⃣ OPTIMIZATION: Vectorized processing
        if len(loans_df) > 1000:  # Use parallel processing for large datasets
            loans_copy = _process_loans_parallel(loans_df, risk_lookup)
        else:
            loans_copy = _process_loans_vectorized(loans_df, risk_lookup)
        
        result[type_key] = loans_copy
        logger.info(f"✅ {type_key} completed in optimized mode")

    logger.info("✅ All loans processed with optimizations")
    return result


def _create_optimized_risk_lookup(cost_risk_df, prepayment_risk_df):
    """
    Create optimized lookup dictionary for fast risk data access
    
    Returns:
        dict: {loan_type: {'dates': [...], 'cost_risk': [...], 'prepay_risk': [...]}}
    """
    # Melt and merge
    cost_long = cost_risk_df.melt(
        id_vars=["Type of Loan"], var_name="Date", value_name="Cost of Risk"
    )
    prepay_long = prepayment_risk_df.melt(
        id_vars=["Type of Loan"], var_name="Date", value_name="Prepayment Risk"
    )
    
    risk_df = pd.merge(cost_long, prepay_long, on=["Type of Loan", "Date"], how="outer")
    risk_df["Date"] = pd.to_datetime(risk_df["Date"], errors="coerce")
    risk_df = risk_df.sort_values(["Type of Loan", "Date"])
    
    # Create optimized lookup dictionary
    risk_lookup = {}
    for loan_type in risk_df["Type of Loan"].unique():
        type_data = risk_df[risk_df["Type of Loan"] == loan_type].copy()
        
        risk_lookup[loan_type] = {
            'dates': type_data["Date"].values,
            'cost_risk': type_data["Cost of Risk"].fillna(0).values,
            'prepay_risk': type_data["Prepayment Risk"].fillna(0).values,
            'date_strings': type_data["Date"].dt.strftime("%Y-%m-%d").values
        }
    
    return risk_lookup


def _process_loans_vectorized(loans_df, risk_lookup):
    """
    Vectorized processing for medium-sized datasets
    """
    loans_copy = loans_df.copy()
    
    # Ensure Maturity Date is datetime
    if "Maturity Date" in loans_copy.columns:
        loans_copy["Maturity Date"] = pd.to_datetime(loans_copy["Maturity Date"], errors="coerce")
    
    # Group by loan type for batch processing
    risk_rates_list = []
    
    for loan_type in loans_copy["Type of Loan"].unique():
        type_mask = loans_copy["Type of Loan"] == loan_type
        type_loans = loans_copy[type_mask]
        
        if loan_type not in risk_lookup:
            # No risk data for this loan type
            risk_rates_batch = [json.dumps({"Date": [], "Cost of Risk": [], "Prepayment Risk": []}) 
                               for _ in range(len(type_loans))]
        else:
            risk_rates_batch = _create_risk_rates_batch(type_loans, risk_lookup[loan_type])
        
        risk_rates_list.extend(risk_rates_batch)
    
    # Assign risk_rates in original order
    loans_copy["risk_rates"] = risk_rates_list
    return loans_copy


def _process_loans_parallel(loans_df, risk_lookup):
    """
    Parallel processing for large datasets
    """
    loans_copy = loans_df.copy()
    
    if "Maturity Date" in loans_copy.columns:
        loans_copy["Maturity Date"] = pd.to_datetime(loans_copy["Maturity Date"], errors="coerce")
    
    # Split into chunks for parallel processing
    chunk_size = max(100, len(loans_copy) // mp.cpu_count())
    chunks = [loans_copy[i:i + chunk_size] for i in range(0, len(loans_copy), chunk_size)]
    
    # Process chunks in parallel
    with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
        processed_chunks = list(executor.map(
            lambda chunk: _process_chunk(chunk, risk_lookup), chunks
        ))
    
    # Combine results
    result_df = pd.concat(processed_chunks, ignore_index=True)
    return result_df


def _process_chunk(chunk_df, risk_lookup):
    """
    Process a chunk of loans
    """
    chunk_copy = chunk_df.copy()
    
    risk_rates_list = []
    for _, loan_row in chunk_copy.iterrows():
        loan_type = loan_row["Type of Loan"]
        maturity_date = loan_row.get("Maturity Date")
        
        if loan_type not in risk_lookup:
            risk_json = json.dumps({"Date": [], "Cost of Risk": [], "Prepayment Risk": []})
        else:
            risk_json = _create_single_risk_json_fast(maturity_date, risk_lookup[loan_type])
        
        risk_rates_list.append(risk_json)
    
    chunk_copy["risk_rates"] = risk_rates_list
    return chunk_copy


def _create_risk_rates_batch(loans_subset, risk_data):
    """
    Create risk_rates for a batch of loans of the same type
    """
    dates = risk_data['dates']
    cost_risk = risk_data['cost_risk']
    prepay_risk = risk_data['prepay_risk']
    date_strings = risk_data['date_strings']
    
    risk_rates_batch = []
    
    for _, loan_row in loans_subset.iterrows():
        maturity_date = loan_row.get("Maturity Date")
        
        if pd.isna(maturity_date):
            # Use all available dates
            filtered_dates = date_strings
            filtered_cost = cost_risk.tolist()
            filtered_prepay = prepay_risk.tolist()
        else:
            # Filter by maturity date using vectorized operations
            mask = dates <= maturity_date
            filtered_dates = date_strings[mask]
            filtered_cost = cost_risk[mask].tolist()
            filtered_prepay = prepay_risk[mask].tolist()
        
        risk_json = json.dumps({
            "Date": filtered_dates.tolist(),
            "Cost of Risk": filtered_cost,
            "Prepayment Risk": filtered_prepay
        })
        
        risk_rates_batch.append(risk_json)
    
    return risk_rates_batch


def _create_single_risk_json_fast(maturity_date, risk_data):
    """
    Fast creation of single risk JSON using pre-computed arrays
    """
    dates = risk_data['dates']
    cost_risk = risk_data['cost_risk']
    prepay_risk = risk_data['prepay_risk']
    date_strings = risk_data['date_strings']
    
    if pd.isna(maturity_date):
        # Use all dates
        return json.dumps({
            "Date": date_strings.tolist(),
            "Cost of Risk": cost_risk.tolist(),
            "Prepayment Risk": prepay_risk.tolist()
        })
    
    # Filter by maturity date
    mask = dates <= maturity_date
    
    return json.dumps({
        "Date": date_strings[mask].tolist(),
        "Cost of Risk": cost_risk[mask].tolist(),
        "Prepayment Risk": prepay_risk[mask].tolist()
    })


# Optional: Progress tracking for very large datasets
def assign_combined_risk_rates_with_progress(combined_loans, cost_risk_df, prepayment_risk_df):
    """
    Version with progress tracking for very large datasets
    """
    try:
        from tqdm import tqdm
        use_progress = True
    except ImportError:
        use_progress = False
    
    logger = logging.getLogger(__name__)
    logger.info("🚀 Starting risk assignment with progress tracking...")
    
    # Same optimization logic but with progress bars
    if isinstance(cost_risk_df, dict):
        cost_risk_df = pd.DataFrame(cost_risk_df).T.reset_index().rename(columns={"index":"Type of Loan"})
    if isinstance(prepayment_risk_df, dict):
        prepayment_risk_df = pd.DataFrame(prepayment_risk_df).T.reset_index().rename(columns={"index":"Type of Loan"})
    
    risk_lookup = _create_optimized_risk_lookup(cost_risk_df, prepayment_risk_df)
    result = {}
    
    total_loans = sum(len(df) for df in combined_loans.values() if df is not None and not df.empty)
    
    if use_progress:
        pbar = tqdm(total=total_loans, desc="Processing loans")
    
    for type_key, loans_df in combined_loans.items():
        if loans_df is None or loans_df.empty:
            result[type_key] = pd.DataFrame()
            continue
        
        if len(loans_df) > 1000:
            loans_copy = _process_loans_parallel(loans_df, risk_lookup)
        else:
            loans_copy = _process_loans_vectorized(loans_df, risk_lookup)
        
        result[type_key] = loans_copy
        
        if use_progress:
            pbar.update(len(loans_df))
    
    if use_progress:
        pbar.close()
    
    logger.info("✅ Processing completed with optimizations")
    return result