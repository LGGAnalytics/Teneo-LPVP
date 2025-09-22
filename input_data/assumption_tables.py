import pandas as pd
import logging
from io import BytesIO

class ExcelTableLoader:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def load_tables_from_sheet(self, excel_file_path: str, sheet_name: str, table_names: list):
        """
        Reads multiple tables from a sheet.
        :param excel_file_path: Excel file path
        :param sheet_name: Sheet name
        :param table_names: List of table names to find in column A
        :return: dict with {table_name: table_dict}
        """
        self.logger.info(f"📥 Loading sheet '{sheet_name}' with tables: {table_names}")
        df_sheet = pd.read_excel(excel_file_path, sheet_name=sheet_name, header=None)
        sheet_dict = {}

        for table_name in table_names:
            self.logger.info(f"Reading table '{table_name}' from sheet '{sheet_name}'")
            table_dict = self._read_single_table(df_sheet, table_name)
            sheet_dict[table_name] = table_dict

        return sheet_dict

    def load_tables_from_stream(self, file_stream, sheet_name: str, table_names: list):
        """
        Reads multiple tables from a sheet using BytesIO stream.
        :param file_stream: BytesIO stream of Excel file
        :param sheet_name: Sheet name
        :param table_names: List of table names to find in column A
        :return: dict with {table_name: table_dict}
        """
        self.logger.info(f"📥 Loading sheet '{sheet_name}' with tables: {table_names}")
        
        try:
            file_stream.seek(0)
            df_sheet = pd.read_excel(file_stream, sheet_name=sheet_name, header=None)
            sheet_dict = {}

            for table_name in table_names:
                self.logger.info(f"Reading table '{table_name}' from sheet '{sheet_name}'")
                table_dict = self._read_single_table(df_sheet, table_name)
                sheet_dict[table_name] = table_dict

            return sheet_dict
            
        except Exception as e:
            self.logger.error(f"Error loading tables from stream: {str(e)}")
            return {}

    def _read_single_table(self, df_sheet: pd.DataFrame, table_name: str):
        """
        Reads a single table in a sheet using table_name in column A.
        Returns dict: {loan_type: {date_col: rate_value, ...}, ...}
        """
        # Bulunan satırı tespit et
        start_idx = None
        for i, val in enumerate(df_sheet.iloc[:, 0]):
            if str(val).strip() == table_name:
                start_idx = i
                break

        if start_idx is None:
            self.logger.warning(f"Table '{table_name}' not found.")
            return {}

        # Başlık satırı ve veri satırlarını al
        header = df_sheet.iloc[start_idx].tolist()  # Başlık satırı (date sütunları)
        data_start = start_idx + 1
        data_end = data_start

        while data_end < len(df_sheet) and not df_sheet.iloc[data_end, :].isnull().all():
            data_end += 1

        df_table = df_sheet.iloc[data_start:data_end].reset_index(drop=True)
        df_table.columns = header

        # Dict'e çevir
        table_dict = {}
        for _, row in df_table.iterrows():
            loan_type = str(row.iloc[0]).strip()  # İlk sütun loan type
            table_dict[loan_type] = {}
            for col in df_table.columns[1:]:
                val = row[col]
                if pd.notna(val):
                    table_dict[loan_type][str(col).strip()] = self._safe_convert_value(val)

        return table_dict

    @staticmethod
    def _safe_convert_value(rate_value):
        """
        Safely convert rate value - keep as percentage format
        
        Excel'de: 10.0000%, -0.5370%, 2.0000%
        Sonuç: 10.0000, -0.5370, 2.0000 (percentage olarak)
        """
        try:
            if isinstance(rate_value, str):
                if '%' in rate_value:
                    # % işaretini kaldır ama değeri olduğu gibi bırak
                    return float(rate_value.replace('%','').strip())
                else:
                    return float(rate_value)
            elif isinstance(rate_value, (int, float)):
                # Eğer Excel'den decimal olarak geldiyse (0.1 = 10%)
                # ve 1'den küçükse percentage'a çevir
                if -1 < rate_value < 1 and rate_value != 0:
                    return rate_value * 100
                else:
                    return rate_value
            else:
                return 0.0
        except:
            return 0.0

# Helper function for file path (existing functionality)
def load_assumptions_excel_to_dict(excel_file_path: str, sheet_tables_dict: dict):
    """
    sheet_tables_dict example:
    {
        "Assumption_Loans": ["Cost of Risk - Loan with Guarantee", "Prepayment Risk - Loan with Guarantee"],
        "Index_Analysis": ["Index Type"]
    }
    """
    loader = ExcelTableLoader()
    result = {}
    for sheet_name, tables in sheet_tables_dict.items():
        sheet_dict = loader.load_tables_from_sheet(excel_file_path, sheet_name, tables)
        result[sheet_name] = sheet_dict
    return result

# Helper function for BytesIO stream (new functionality)
def load_assumptions_excel_to_dict_from_stream(file_stream, sheet_tables_dict: dict):
    """
    Load assumptions from BytesIO stream
    sheet_tables_dict example:
    {
        "Assumption_Loans": ["Cost of Risk - Loan with Guarantee", "Prepayment Risk - Loan with Guarantee"],
        "Index_Analysis": ["Index Type"]
    }
    """
    loader = ExcelTableLoader()
    result = {}
    for sheet_name, tables in sheet_tables_dict.items():
        sheet_dict = loader.load_tables_from_stream(file_stream, sheet_name, tables)
        result[sheet_name] = sheet_dict
    return result