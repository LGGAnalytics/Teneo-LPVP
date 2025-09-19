import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def convert_margin_to_decimal(margin_value):
    """
    Convert loan margin or interest rate to decimal
    """
    try:
        if pd.isna(margin_value) or margin_value in ['Not available', 'Not applicable', '', 'n/a', 'na']:
            return 0.0
        if isinstance(margin_value, str) and '%' in margin_value:
            return float(margin_value.replace('%','').strip()) / 100
        val = float(margin_value)
        return val / 100 if val > 1 else val
    except:
        return 0.0

def process_floating_calculations(floating_df, assumptions_dict, excel_filename=None):
    floating_df = floating_df.copy()
    floating_df['total_rates'] = [{} for _ in range(len(floating_df))]  # * yerine _
    
    # Percentage formatında bırakıyoruz, decimal'a çevirmiyoruz
    for index_name, group_df in floating_df.groupby('Index'):
        index_rates = assumptions_dict.get(index_name, {})
        if not index_rates:
            continue
        for i, loan in group_df.iterrows():
            total_rates = {}
            maturity = loan.get('Maturity Date')
            margin = loan['Interest Rate Margin (%)']  # 2.0 (percentage)
            
            if pd.isna(maturity):
                continue
            for period_date, assumption_rate in index_rates.items():
                if period_date <= str(maturity):
                    # assumption_rate (-0.5095%) + margin (2%) = 1.4905%
                    total_rates[period_date] = assumption_rate + margin
            
            floating_df.at[i, 'total_rates'] = total_rates
    
    if excel_filename:
        floating_df.to_excel(excel_filename, index=False)
        logging.info(f"Floating DataFrame exported to {excel_filename}")
    
    return floating_df

def export_floating_to_excel(floating_df, filename='floating_loans.xlsx'):
    """
    Export floating DataFrame to Excel, keeping all original columns and total_rates dictionary
    """
    floating_df.to_excel(filename, index=False)
    logging.info(f"Floating DataFrame exported to {filename}")
    return filename

def print_floating_summary(floating_df):
    """
    Print basic summary of floating loans
    """
    total_loans = len(floating_df)
    total_periods = sum(len(v) for v in floating_df['total_rates'])
    return {
        'total_loans': total_loans,
        'total_periods': total_periods
    }

def validate_floating_rates(floating_df):
    """
    Validate total_rates for negative, zero, or extreme (>100%) rates
    """
    report = {'total_loans':0, 'total_periods':0, 'negative_rates':0, 'zero_rates':0, 'extreme_rates':0}
    report['total_loans'] = len(floating_df)
    for rates in floating_df['total_rates']:
        report['total_periods'] += len(rates)
        for r in rates.values():
            if r < 0: report['negative_rates'] += 1
            if r == 0: report['zero_rates'] += 1
            if r > 1: report['extreme_rates'] += 1
    return report
