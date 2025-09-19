import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def group_by_guarantees(df):
    df['Guarantee current value'] = pd.to_numeric(df['Guarantee current value'], errors='coerce').fillna(0)
    aggregated = df.groupby('Unique Loan ID', as_index=False)['Guarantee current value'].max()
    with_guarantees_ids = aggregated[aggregated['Guarantee current value']>0]['Unique Loan ID']
    return with_guarantees_ids

def group_by_calculation_type(pl_df):
    calculation_types = {
        "Medium / Long Term Loan": '1',
        "RE Leasing": '1',
        "Overdraft": "Special",
        "Syndicated Loan": '1',
        "Factoring": '1',
        "Residential Mortgage": '1',
        "Credit Card": "Special",
        "Corporate/ Development Loan": '1',
        "Current Account": "Special",
        "Non RE Leasing": '1',
        "Discounted Bill/ Note": "2",
        "Consumer Loan": '1',
        "Other": '1',
        "Trade Finance": '1',
        "Restructured Loan": '1',
        "Uncalled Bank Guarantee": 'Asset',
        "Called Bank Guarantee": 'non-performing'
    }

    pl_df['Type of Loan'] = pl_df['Type of Loan'].astype(str).str.strip()
    pl_df['Type of Calculation'] = pl_df['Type of Loan'].map(calculation_types).fillna('Not Found')
    grouped = {k: v.reset_index(drop=True).copy() for k, v in pl_df.groupby('Type of Calculation')}
    return grouped

def group_by_rate_type(df):
    mask = df['Interest Rate Type'] == 'Floating'
    floating_dataset = df[mask].copy()
    nonfloating_dataset = df[~mask].copy()
    return floating_dataset, nonfloating_dataset

def check_problematic_loans(pl_dataset, excel_file_path):
    problematic_values = ['not available', 'not applicable', '', 'nan', 'null']
    special_loan_types = ['Current Account', 'Overdraft', 'Credit Card']
    problematic_loans = []

    for _, loan in pl_dataset.iterrows():
        loan_id = loan.get('Unique Loan ID', 'UNKNOWN')
        maturity_date = loan.get('Maturity Date', '')
        loan_type = loan.get('Type of Loan', '')
        outstanding_balance = loan.get('Outstanding Balance After Adjustments', '')

        maturity_problematic = pd.isna(maturity_date) or (isinstance(maturity_date, str) and maturity_date.lower().strip() in problematic_values)
        loan_type_not_special = not (isinstance(loan_type, str) and loan_type.strip() in special_loan_types)
        outstanding_problematic = pd.isna(outstanding_balance) or (isinstance(outstanding_balance, str) and outstanding_balance.lower().strip() in problematic_values) or outstanding_balance == 0

        if maturity_problematic and loan_type_not_special and outstanding_problematic:
            problematic_loans.append({'Unique Loan ID': loan_id, 'Loan Data': loan.to_dict()})

    if problematic_loans:
        problematic_df = pd.DataFrame([loan['Loan Data'] for loan in problematic_loans])
        return problematic_df
    return pd.DataFrame()

def split_npls(NPL_dataset, excel_file_path):
    filename = os.path.basename(excel_file_path).lower()
    try:
        if 'complex' in filename:
            guarantees_df = pd.read_excel(excel_file_path, sheet_name='GuaranteesConso')
            guarantees_df.columns = guarantees_df.columns.str.strip()
            npl_guarantees_df = guarantees_df[guarantees_df['Unique Loan ID'].isin(NPL_dataset['Unique Loan ID'])].copy()
            with_guarantees_ids = group_by_guarantees(npl_guarantees_df)
        else:
            with_guarantees_ids = group_by_guarantees(NPL_dataset)

        NPL_with_guarantees = NPL_dataset[NPL_dataset['Unique Loan ID'].isin(with_guarantees_ids)].copy()
        NPL_noguarantees = NPL_dataset[~NPL_dataset['Unique Loan ID'].isin(with_guarantees_ids)].copy()
        return NPL_with_guarantees, NPL_noguarantees
    except Exception as e:
        raise RuntimeError(f"Failed to split NPLs: {e}")

def split_pls(PL_dataset):
    grouped = group_by_calculation_type(PL_dataset)
    f1 = f2 = f3 = f4 = asset_loans = nf1 = nf2 = nf3 = nf4 = pd.DataFrame()
    special_loans = pd.DataFrame()

    for i in range(1, 5):
        df = grouped.get(str(i))
        if df is not None and not df.empty:
            floating_df, non_floating_df = group_by_rate_type(df)
            if i == 1:
                f1, nf1 = floating_df, non_floating_df
            elif i == 2:
                f2, nf2 = floating_df, non_floating_df
            elif i == 3:
                f3, nf3 = floating_df, non_floating_df
            elif i == 4:
                f4, nf4 = floating_df, non_floating_df

    asset_df = grouped.get('Asset')
    if asset_df is not None and not asset_df.empty:
        asset_loans = asset_df.copy()

    special_df = grouped.get('Special')
    if special_df is not None and not special_df.empty:
        special_loans = special_df.copy()

    return {
        'original': {
            'floating': {'f1': f1, 'f2': f2, 'f3': f3, 'f4': f4},
            'fixed': {'nf1': nf1, 'nf2': nf2, 'nf3': nf3, 'nf4': nf4},
            'assets': asset_loans,
            'special': special_loans
        },
        'optimized': {}  # artık index bazlı grup yok
    }

def process_loans_dataframe_segmentation(datatape_df, excel_file_path):
    datatape_df.columns = datatape_df.columns.str.strip()
    if 'Past Due Date' in datatape_df.columns:
        datatape_df['Past Due Date'] = pd.to_datetime(datatape_df['Past Due Date'], errors='coerce').fillna(0)
        mask = datatape_df['Past Due Date'] == 0
        PL_dataset = datatape_df[mask].copy()
        NPL_dataset = datatape_df[~mask].copy()
    else:
        PL_dataset = datatape_df.copy()
        NPL_dataset = pd.DataFrame()

    NPL_with_guarantees = NPL_noguarantees = pd.DataFrame()
    if not NPL_dataset.empty:
        NPL_with_guarantees, NPL_noguarantees = split_npls(NPL_dataset, excel_file_path)

    problematic_loans = check_problematic_loans(PL_dataset, excel_file_path)
    if not problematic_loans.empty:
        loan_ids_to_remove = problematic_loans['Unique Loan ID'].tolist()
        PL_dataset = PL_dataset[~PL_dataset['Unique Loan ID'].isin(loan_ids_to_remove)].copy()

    pl_results = split_pls(PL_dataset)

    complete_results = {
        'performing_loans': pl_results,
        'non_performing_loans': {
            'with_guarantees': NPL_with_guarantees,
            'without_guarantees': NPL_noguarantees
        },
        'problematic_loans': problematic_loans,
        'summary': {
            'total_loans': len(datatape_df),
            'performing_count': len(PL_dataset),
            'non_performing_count': len(NPL_dataset),
            'npl_with_guarantees': len(NPL_with_guarantees),
            'npl_without_guarantees': len(NPL_noguarantees),
            'problematic_loans_count': len(problematic_loans)
        }
    }

    return complete_results

if __name__ == "__main__":
    logging.info("datatape_segmentation.py is meant to be imported; run loan_input.py for main pipeline")
