import pandas as pd
import logging

def load_loans_excel(excel_file_path):
    """
    Load loans Excel with percentage fix for rate columns
    """
    logging.info(f"📥 Loading loans Excel from: {excel_file_path}")
    
    try:
        # Percentage converter function
        def percentage_converter(value):
            if pd.isna(value):
                return value
            if isinstance(value, (int, float)) and 0 <= value < 1:
                return value * 100
            return value
        
        # Define converters for percentage columns
        converters = {
            'Interest Rate (%)': percentage_converter,
            'Interest Rate Margin (%)': percentage_converter
        }
        
        # Load Excel with converters
        loans_df = pd.read_excel(
            excel_file_path, 
            sheet_name="Loans",
            converters=converters
        )
        
        # Clean column names
        loans_df.columns = loans_df.columns.str.strip()
        
        logging.info(f"✅ Loans Excel loaded successfully. Shape={loans_df.shape}")
        return loans_df
        
    except Exception as e:
        logging.error(f"❌ Failed to load loans Excel: {e}", exc_info=True)
        raise