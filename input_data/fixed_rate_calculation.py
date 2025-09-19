import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def convert_rate_to_decimal(rate_value):
    """
    Convert a rate (string with %, float, or NaN) to decimal format
    """
    try:
        if pd.isna(rate_value) or rate_value in ['Not available', 'Not applicable', '', 'n/a', 'na']:
            return 0.0
        if isinstance(rate_value, str) and '%' in rate_value:
            return float(rate_value.replace('%','').strip()) / 100
        val = float(rate_value)
        if val > 1:
            val /= 100
        return val
    except Exception as e:
        logging.warning(f"Failed to convert rate {rate_value}: {e}")
        return 0.0


def process_fixed_loans_dataframe(loans_df, type_key):
    """
    Process a single fixed loan DataFrame and return the same DataFrame with an extra column 'total_rates'
    total_rates: {maturity_date: fixed_rate_decimal}
    """
    loans_df = loans_df.copy()

    if loans_df.empty:
        loans_df['total_rates'] = [{} for _ in range(len(loans_df))]
        logging.info(f"Type {type_key} fixed loans DataFrame is empty, added empty total_rates column")
        return loans_df

    total_rates_list = []

    logging.info(f"Starting fixed calculations for Type {type_key} on {len(loans_df)} loans...")

    for _, loan in loans_df.iterrows():
        interest_rate = loan.get('Interest Rate (%)', 0)
        maturity_date = loan.get('Maturity Date', None)

        # Convert interest rate to decimal
        rate_decimal = convert_rate_to_decimal(interest_rate)

        # Create total_rates dictionary
        if pd.isna(maturity_date) or maturity_date is None:
            total_rate_dict = {}
        else:
            total_rate_dict = {str(maturity_date): rate_decimal}

        total_rates_list.append(total_rate_dict)

    loans_df['total_rates'] = total_rates_list

    logging.info(f"Completed fixed calculations for Type {type_key}")
    return loans_df


def process_fixed_calculations(fixed_dfs):
    """
    Process all fixed loans grouped by type.
    Input: dict of DataFrames by type_key (e.g., {"nf1": df1, "nf2": df2, ...})
    Output: dict of DataFrames with 'total_rates' column
    """
    fixed_results = {}

    for type_key, df in fixed_dfs.items():
        logging.info(f"Processing Type {type_key} fixed loans: {len(df)} loans")
        df_with_rates = process_fixed_loans_dataframe(df, type_key)
        fixed_results[type_key] = df_with_rates
        logging.info(f"Type {type_key} fixed loans processed successfully with total_rates column")

    return fixed_results


def print_fixed_calculation_summary(fixed_results):
    """
    Print detailed summary of fixed loans
    """
    for type_key, df in fixed_results.items():
        logging.info(f"\n=== TYPE {type_key} FIXED CALCULATIONS SUMMARY ===")
        logging.info(f"Total loans: {len(df)}")
        if not df.empty:
            rates = [list(r.values())[0] if r else 0.0 for r in df['total_rates']]
            avg_rate = sum(rates)/len(rates)
            min_rate = min(rates)
            max_rate = max(rates)
            logging.info(f"Average rate: {avg_rate*100:.4f}%")
            logging.info(f"Min rate: {min_rate*100:.4f}%")
            logging.info(f"Max rate: {max_rate*100:.4f}%")

            # Show 5 sample loans
            logging.info("Sample total_rates values:")
            for i, r in enumerate(df['total_rates'].head(5)):
                logging.info(f"  Loan {i+1}: {r}")
