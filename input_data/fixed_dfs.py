import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor

# ---------- Helper Functions ----------
def extract_tables_from_sheet(raw_excel_data: pd.DataFrame):
    raw_excel_data['row_index'] = range(len(raw_excel_data))
    is_empty = raw_excel_data.drop(columns='row_index').isnull().all(axis=1)

    table_ranges, start = [], None
    for idx in range(len(raw_excel_data)):
        row_is_empty = is_empty.iloc[idx]
        if not row_is_empty and start is None:
            start = idx
        elif row_is_empty and start is not None:
            table_ranges.append((start, idx))
            start = None
    if start is not None:
        table_ranges.append((start, len(raw_excel_data)))

    def drop_trailing_nan_cols(df):
        non_nan_cols = df.columns[~df.isnull().all()]
        if non_nan_cols.empty:
            return pd.DataFrame()
        last_valid_idx = df.columns.get_loc(non_nan_cols[-1])
        return df.iloc[:, :last_valid_idx + 1]

    tables = []
    for start, end in table_ranges:
        chunk = raw_excel_data.iloc[start:end].drop(columns='row_index')
        chunk.columns = chunk.iloc[0]
        df = chunk[1:].reset_index(drop=True)
        df.columns.name = None
        df = drop_trailing_nan_cols(df)
        df = df.reset_index(drop=True)
        tables.append(df)
    
    return tables

# ---------- Fixed Assumptions Loaders ----------
def get_fixed_summary_assumptions(assump_path: str, fix_percentage=False, debug_mode=False):
    summary_table = pd.read_excel(assump_path, sheet_name='Assumption_Summary', header=0)
    
    # Percentage sütunları - bunlar summary'den geliyor
    percentage_fields = ['global_tax', 'cor_spread', 'dr_sensitivity_var', 'dr_sensitivity_range', 
                        'cr_sensitivity_var', 'cr_sensitivity_range']
    
    data = {
        'val_date': summary_table.loc[summary_table['Assumption Summary'] == 'Valuation Date', 'Input'].iloc[0],
        'output_currency': summary_table.loc[summary_table['Assumption Summary'] == 'Output conclusions display currency', 'Input'].iloc[0],
        'global_tax_flag': summary_table.loc[summary_table['Assumption Summary'] == 'Global Tax Flag (Performing Loans only)', 'Input'].iloc[0],
        'global_tax': summary_table.loc[summary_table['Assumption Summary'] == 'Global Tax (Performing Loans only)', 'Input'].iloc[0],
        'cor_spread': summary_table.loc[summary_table['Assumption Summary'] == 'Assumption: % of the initial debt to be repaid - minimum Credit Card Monthly Payment', 'Input'].iloc[0],
        'dr_sensitivity_var': summary_table.loc[summary_table['Assumption Summary'] == 'Discount Rate Sensitivity Variance', 'Input'].iloc[0],
        'dr_sensitivity_range': summary_table.loc[summary_table['Assumption Summary'] == 'Sensitivity Table: Discount Rate Sensitivity Range', 'Input'].iloc[0],
        'cr_sensitivity_var': summary_table.loc[summary_table['Assumption Summary'] == 'Cost of Risk Sensitivity Variance', 'Input'].iloc[0],
        'cr_sensitivity_range': summary_table.loc[summary_table['Assumption Summary'] == 'Sensitivity Table: Cost of Risk Sensitivity Range', 'Input'].iloc[0]  
    }
    
    # DEBUG: Summary percentage değerlerini kontrol et
    if debug_mode:
        print("\n=== 🔍 DEBUG: SUMMARY PERCENTAGE VALUES ===")
        for field in percentage_fields:
            value = data[field]
            print(f"{field}: {value} (type: {type(value).__name__})")
            if isinstance(value, (int, float)) and value < 1:
                print(f"  ⚠️  {field} appears to be in decimal format")
    
    # FIX: Summary percentage değerlerini düzelt
    if fix_percentage:
        print("\n=== 🔧 APPLYING SUMMARY PERCENTAGE FIX ===")
        for field in percentage_fields:
            value = data[field]
            if isinstance(value, (int, float)) and value < 1:
                old_value = value
                data[field] = value * 100
                if debug_mode:
                    print(f"  🔧 Converting {field}: {old_value} -> {data[field]}")
            elif debug_mode:
                print(f"  ✅ {field}: Already in percentage format ({value})")
    
    return pd.DataFrame([data])

def load_assumptions_once(assump_path: str):
    # FX / Tax
    assump_currency_raw = pd.read_excel(assump_path, sheet_name='Assumption_Currency', header=None)
    assump_currency_tables = extract_tables_from_sheet(assump_currency_raw)
    fx_table, tax_table = assump_currency_tables[0], assump_currency_tables[1]

    # Rates & Fees
    assump_loans_raw = pd.read_excel(assump_path, sheet_name='Assumption_Loans', header=None)
    assump_loans_tables = extract_tables_from_sheet(assump_loans_raw)
    rates_fees_table = assump_loans_tables[2]

    return fx_table, tax_table, rates_fees_table

def prepare_rates_fees_dict(rates_fees_table, debug_mode=False):
    calculation_types = {
        "Medium / Long Term Loan": '1', "RE Leasing": '1', "Overdraft": "3", "Syndicated Loan": '1',
        "Factoring": '1', "Residential Mortgage": '1', "Credit Card": "4", "Corporate/ Development Loan": '1',
        "Current Account": "3", "Non RE Leasing": '1', "Discounted Bill/ Note": "2", "Consumer Loan": '1',
        "Other": '1', "Trade Finance": '1', "Restructured Loan": '1'
    }

    rates_fees_table['Calculation Type'] = rates_fees_table['Types of Loans'].map(calculation_types).fillna('')

    # DEBUG: Percentage değerlerini kontrol et
    if debug_mode:
        print("\n=== 🔍 DEBUG: PERCENTAGE VALUES ===")
        percentage_columns = ['Discount Rate', 'Non-interest fees (over undrawn commitment)', 
                            'Non-interest fees (over outstanding balance)', 'Servicing Fee']
        
        for col in percentage_columns:
            if col in rates_fees_table.columns:
                sample_values = rates_fees_table[col].dropna().head(3)
                print(f"\n{col} sample values:")
                for idx, val in sample_values.items():
                    print(f"  Row {idx}: {val} (type: {type(val).__name__})")
                
                # Percentage format kontrolü
                non_null_values = rates_fees_table[col].dropna()
                if len(non_null_values) > 0:
                    max_val = non_null_values.max()
                    if max_val < 1:
                        print(f"  ⚠️  {col}: Values appear to be in decimal format (max: {max_val})")
                        print(f"  📝 Excel probably shows as percentage but pandas reads as decimal")
                    else:
                        print(f"  ✅ {col}: Values appear to be in percentage format (max: {max_val})")

    rates_fees_dict = {}
    for type_id in range(1,5):
        filtered_df = rates_fees_table[rates_fees_table['Calculation Type'] == str(type_id)].reset_index(drop=True)
        filtered_df = filtered_df.drop(columns=['Calculation Type'])
        
        rates_fees_dict[type_id] = {
            'discount_rate': dict(zip(filtered_df['Types of Loans'], filtered_df['Discount Rate'])),
            'fees_undrawn_commitment': dict(zip(filtered_df['Types of Loans'], filtered_df['Non-interest fees (over undrawn commitment)'])),
            'fees_outstanding_balance': dict(zip(filtered_df['Types of Loans'], filtered_df['Non-interest fees (over outstanding balance)'])),
            'servicing_fee': dict(zip(filtered_df['Types of Loans'], filtered_df['Servicing Fee']))
        }
        
        # DEBUG: Dict'e eklenen değerleri göster
        if debug_mode and type_id == 1:  # Sadece type_1 için göster
            print(f"\n=== 📊 DEBUG: TYPE_{type_id} DICT VALUES ===")
            sample_loan_types = list(rates_fees_dict[type_id]['discount_rate'].keys())[:3]
            for loan_type in sample_loan_types:
                dr = rates_fees_dict[type_id]['discount_rate'][loan_type]
                print(f"  {loan_type}: discount_rate = {dr} (type: {type(dr).__name__})")
    
    return rates_fees_dict

# FIX'Li versiyonu
def prepare_rates_fees_dict_with_percentage_fix(rates_fees_table, debug_mode=False):
    """
    Percentage formatını düzelten versiyonu
    """
    calculation_types = {
        "Medium / Long Term Loan": '1', "RE Leasing": '1', "Overdraft": "3", "Syndicated Loan": '1',
        "Factoring": '1', "Residential Mortgage": '1', "Credit Card": "4", "Corporate/ Development Loan": '1',
        "Current Account": "3", "Non RE Leasing": '1', "Discounted Bill/ Note": "2", "Consumer Loan": '1',
        "Other": '1', "Trade Finance": '1', "Restructured Loan": '1'
    }

    rates_fees_table = rates_fees_table.copy()
    rates_fees_table['Calculation Type'] = rates_fees_table['Types of Loans'].map(calculation_types).fillna('')

    # Percentage sütunlarını belirle
    percentage_columns = ['Discount Rate', 'Non-interest fees (over undrawn commitment)', 
                         'Non-interest fees (over outstanding balance)', 'Servicing Fee']

    # Percentage formatını düzelt
    if debug_mode:
        print("\n=== 🔧 APPLYING PERCENTAGE FIX ===")
    
    for col in percentage_columns:
        if col in rates_fees_table.columns:
            non_null_values = rates_fees_table[col].dropna()
            if len(non_null_values) > 0 and (non_null_values < 1).all():
                if debug_mode:
                    print(f"  🔧 Converting {col}: {non_null_values.iloc[0]} -> {non_null_values.iloc[0] * 100}")
                rates_fees_table[col] = rates_fees_table[col] * 100
            elif debug_mode:
                print(f"  ✅ {col}: Already in percentage format")

    rates_fees_dict = {}
    for type_id in range(1,5):
        filtered_df = rates_fees_table[rates_fees_table['Calculation Type'] == str(type_id)].reset_index(drop=True)
        filtered_df = filtered_df.drop(columns=['Calculation Type'])
        
        rates_fees_dict[type_id] = {
            'discount_rate': dict(zip(filtered_df['Types of Loans'], filtered_df['Discount Rate'])),
            'fees_undrawn_commitment': dict(zip(filtered_df['Types of Loans'], filtered_df['Non-interest fees (over undrawn commitment)'])),
            'fees_outstanding_balance': dict(zip(filtered_df['Types of Loans'], filtered_df['Non-interest fees (over outstanding balance)'])),
            'servicing_fee': dict(zip(filtered_df['Types of Loans'], filtered_df['Servicing Fee']))
        }
        
        # DEBUG: Düzeltilmiş değerleri göster
        if debug_mode and type_id == 1:
            print(f"\n=== ✅ DEBUG: FIXED TYPE_{type_id} VALUES ===")
            sample_loan_types = list(rates_fees_dict[type_id]['discount_rate'].keys())[:3]
            for loan_type in sample_loan_types:
                dr = rates_fees_dict[type_id]['discount_rate'][loan_type]
                print(f"  {loan_type}: discount_rate = {dr} (type: {type(dr).__name__})")
    
    return rates_fees_dict

# ---------- Type Processing ----------
def process_type(loans_df, type_id, summary_df, fx_table, tax_table, rates_fees_dict, fix_percentage=False, debug_mode=False):
    if loans_df is None or loans_df.empty:
        return pd.DataFrame()

    # Ensure columns exist
    loans_df = loans_df.copy()
    loans_df["Currency"] = loans_df.get("Currency", "EUR")
    loans_df["Type of Loan"] = loans_df.get("Type of Loan", "Other")
    if "Maturity Date" not in loans_df.columns and "maturity_date" in loans_df.columns:
        loans_df["Maturity Date"] = loans_df["maturity_date"]
    loans_df["Outstanding Balance After Adjustments (€)"] = loans_df.get("Outstanding Balance After Adjustments (€)", 0.0)

    # DEBUG: Interest Rate sütunu kontrol et (bu loans_df'den geliyor)
    if debug_mode and "Interest Rate (%)" in loans_df.columns:
        print(f"\n=== 🔍 DEBUG: TYPE_{type_id} INTEREST RATE VALUES ===")
        sample_rates = loans_df["Interest Rate (%)"].dropna().head(3)
        for idx, rate in sample_rates.items():
            print(f"  Row {idx}: {rate} (type: {type(rate).__name__})")
        
        max_rate = loans_df["Interest Rate (%)"].max()
        if max_rate < 1:
            print(f"  ⚠️  Interest Rate (%): Values appear to be in decimal format (max: {max_rate})")
        else:
            print(f"  ✅ Interest Rate (%): Values appear to be in percentage format (max: {max_rate})")

    # FIX: Interest Rate sütunu düzelt
    if fix_percentage and "Interest Rate (%)" in loans_df.columns:
        interest_rates = loans_df["Interest Rate (%)"]
        if interest_rates.notna().any() and (interest_rates[interest_rates.notna()] < 1).all():
            if debug_mode:
                old_sample = interest_rates.dropna().iloc[0]
                print(f"\n=== 🔧 FIXING TYPE_{type_id} INTEREST RATES ===")
                print(f"  🔧 Converting Interest Rate (%): {old_sample} -> {old_sample * 100}")
            loans_df["Interest Rate (%)"] = loans_df["Interest Rate (%)"] * 100

    output_currency = summary_df['output_currency'].iloc[0]

    # FX / Tax mapping
    fx_dict, tax_dict = {}, {}
    unique_currencies = loans_df['Currency'].dropna().unique()
    for cur in unique_currencies:
        rate_row = fx_table[(fx_table['Quote Currency']==cur) & (fx_table['Base Currency']==output_currency)]
        fx_dict[cur] = rate_row.iloc[0]['Exchange Rate at Valuation Date'] if not rate_row.empty else 1.0
        tax_row = tax_table[tax_table['Local Currency (Performing Loans only)']==cur]
        tax_dict[cur] = tax_row.iloc[0]['Corporate Tax'] if not tax_row.empty else 0.0

    # Add fixed columns (artık düzeltilmiş summary değerleri)
    loans_df['val_date'] = summary_df['val_date'].iloc[0]
    loans_df['output_currency'] = output_currency
    loans_df['global_tax_flag'] = summary_df['global_tax_flag'].iloc[0]
    loans_df['global_tax'] = summary_df['global_tax'].iloc[0]
    loans_df['cor_spread'] = summary_df['cor_spread'].iloc[0]
    loans_df['dr_sensitivity_var'] = summary_df['dr_sensitivity_var'].iloc[0]
    loans_df['dr_sensitivity_range'] = summary_df['dr_sensitivity_range'].iloc[0]
    loans_df['cr_sensitivity_var'] = summary_df['cr_sensitivity_var'].iloc[0]
    loans_df['cr_sensitivity_range'] = summary_df['cr_sensitivity_range'].iloc[0]

    loans_df['fx_rate'] = loans_df['Currency'].map(fx_dict)
    loans_df['tax_rate'] = loans_df['Currency'].map(tax_dict)

    rates = rates_fees_dict[type_id]
    loans_df['discount_rate'] = loans_df['Type of Loan'].map(rates['discount_rate'])
    loans_df['fees_undrawn_commitment'] = loans_df['Type of Loan'].map(rates['fees_undrawn_commitment'])
    loans_df['fees_outstanding_balance'] = loans_df['Type of Loan'].map(rates['fees_outstanding_balance'])
    loans_df['servicing_fee'] = loans_df['Type of Loan'].map(rates['servicing_fee'])

    return loans_df

# ---------- Central Function with Debug ----------
def enrich_loans_with_fixed_assumptions_parallel(loans_dict: dict, assumptions_excel_path: str, 
                                               debug_percentage=False, fix_percentage=False) -> dict:
    """
    Enhanced version with percentage debugging and fixing options
    
    Args:
        loans_dict: Dictionary of loans by type
        assumptions_excel_path: Path to assumptions Excel file
        debug_percentage: Enable percentage format debugging
        fix_percentage: Apply automatic percentage fix (0.14 -> 14)
    
    Returns:
        dict: Enriched loans dictionary
    """
    print(f"\n🚀 Starting loan enrichment with debug_percentage={debug_percentage}, fix_percentage={fix_percentage}")
    
    # UPDATED: Summary assumptions'ı da fix parametreleri ile oku
    summary_df = get_fixed_summary_assumptions(assumptions_excel_path, fix_percentage, debug_percentage)
    fx_table, tax_table, rates_fees_table = load_assumptions_once(assumptions_excel_path)
    
    if fix_percentage:
        print("🔧 Using percentage fix version...")
        rates_fees_dict = prepare_rates_fees_dict_with_percentage_fix(rates_fees_table, debug_percentage)
    else:
        print("📊 Using original version...")
        rates_fees_dict = prepare_rates_fees_dict(rates_fees_table, debug_percentage)

    # Final debug: Çıkan değerleri göster
    if debug_percentage:
        print(f"\n=== 🎯 FINAL DEBUG: SAMPLE OUTPUT VALUES ===")
        sample_type = 1
        sample_loan_type = list(rates_fees_dict[sample_type]['discount_rate'].keys())[0]
        sample_discount_rate = rates_fees_dict[sample_type]['discount_rate'][sample_loan_type]
        print(f"Sample: Type {sample_type}, {sample_loan_type}")
        print(f"Discount Rate: {sample_discount_rate} (type: {type(sample_discount_rate).__name__})")
        
        # Summary değerlerini de göster
        print(f"\nSample Summary Values:")
        print(f"Global Tax: {summary_df['global_tax'].iloc[0]}")
        print(f"CoR Spread: {summary_df['cor_spread'].iloc[0]}")
        print(f"DR Sensitivity Range: {summary_df['dr_sensitivity_range'].iloc[0]}")
        
        if isinstance(sample_discount_rate, (int, float)):
            if sample_discount_rate < 1:
                print("❌ Still in decimal format (e.g., 0.14 for 14%)")
            else:
                print("✅ In percentage format (e.g., 14 for 14%)")

    enriched_loans = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}
        for type_id in range(1,5):
            type_key = f"type_{type_id}"
            # UPDATED: process_type'a da debug ve fix parametrelerini geç
            futures[executor.submit(process_type, loans_dict.get(type_key), type_id, summary_df, fx_table, tax_table, rates_fees_dict, fix_percentage, debug_percentage)] = type_key

        for fut in futures:
            type_key = futures[fut]
            enriched_loans[type_key] = fut.result()

    print("✅ Loan enrichment completed!")
    return enriched_loans