import logging  
import os  
import pandas as pd 
from typing import Optional, Dict, Union  
from sqlalchemy import create_engine  
from google.oauth2 import service_account  
from googleapiclient.discovery import build  

# Konfigurasi Logging  
logging.basicConfig(  
    level=logging.INFO,   
    format='%(asctime)s - %(levelname)s: %(message)s'  
)  
logger = logging.getLogger(__name__)  

class DataLoader:  
    def __init__(  
        self,  
        csv_path: Optional[str] = None,  
        google_credentials: Optional[str] = None  
    ):  
        try:  
            # Default path jika tidak disediakan  
            if csv_path is None:  
                csv_path = os.path.join(os.getcwd(), 'products.csv')  
            
            # Default credentials jika tidak disediakan  
            if google_credentials is None:  
                google_credentials = os.path.join(os.getcwd(), 'google-sheets-api.json')  
            
            # Pastikan direktori ada  
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)  
            
            self.csv_path = os.path.abspath(csv_path)  
            self.google_credentials = os.path.abspath(google_credentials)  
            
            logger.info(f"Inisialisasi DataLoader dengan path: {self.csv_path}")  
        
        except Exception as e:  
            logger.error(f"Gagal menginisialisasi DataLoader: {e}")  
            raise  

    def save_to_csv(  
        self,  
        df: pd.DataFrame,  
        filename: Optional[str] = None  
    ) -> bool:  
        try:  
            # Validasi input  
            if df is None or df.empty:  
                logger.warning("DataFrame kosong atau None")  
                return False  
            
            # Validasi tipe DataFrame  
            if not isinstance(df, pd.DataFrame):  
                raise TypeError("Input harus DataFrame pandas")  
            
            # Validasi kolom wajib  
            required_columns = ['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender']  
            missing_columns = [col for col in required_columns if col not in df.columns]  
            
            if missing_columns:  
                logger.warning(f"Kolom hilang: {missing_columns}")  
                return False  
            
            # Tentukan path file  
            save_path = filename or self.csv_path  
            
            # Buat direktori jika belum ada  
            os.makedirs(os.path.dirname(save_path), exist_ok=True)  
            
            # Simpan ke CSV  
            df.to_csv(save_path, index=False)  
            
            logger.info(f"Data berhasil disimpan ke {save_path}")  
            return True  
        
        except Exception as e:  
            logger.error(f"Gagal menyimpan ke CSV: {e}")  
            return False  

    def save_to_postgresql(  
        self,   
        df: pd.DataFrame,   
        connection_string: str,  
        table_name: str = 'fashion_products'  
    ) -> bool:  
        try:  
            # Validasi input  
            if df is None or df.empty: 
                logger.warning("DataFrame kosong atau None")  
                return False  
            
            # Pastikan connection_string adalah string  
            if isinstance(connection_string, tuple):  
                connection_string = connection_string[0]

            # Buat koneksi engine  
            engine = create_engine(connection_string)  
            
            # Simpan ke database  
            df.to_sql(  
                name=table_name,   
                con=engine,   
                if_exists='replace',   
                index=False  
            )  
        
            logger.info(f"Data berhasil disimpan ke tabel {table_name}")  
            return True  
        
        except Exception as e:  
            logger.error(f"Gagal menyimpan ke PostgreSQL: {e}")  
            return False  

    def save_to_google_sheets(  
        self,   
        df: pd.DataFrame,   
        spreadsheet_id: str,  
        range_name: str = 'Sheet1!A1'  
    ) -> bool:  
        try:  
            # Validasi input  
            if df is None or df.empty:  
                logger.warning("DataFrame kosong atau None")  
                return False  
            
            # Validasi kredensial  
            if not os.path.exists(self.google_credentials):  
                logger.error("Kredensial Google Sheets tidak ditemukan")  
                return False  
            
            # Autentikasi  
            credentials = service_account.Credentials.from_service_account_file(  
                self.google_credentials,  
                scopes=['https://www.googleapis.com/auth/spreadsheets']  
            )  
            
            # Bangun layanan Google Sheets  
            service = build('sheets', 'v4', credentials=credentials)  
            
            # Konversi DataFrame ke format yang dapat ditulis  
            values = [df.columns.tolist()] + df.values.tolist()  
            
            # Perbarui spreadsheet  
            request_body = {'values': values}  
            service.spreadsheets().values().update(  
                spreadsheetId=spreadsheet_id,  
                range=range_name,  
                valueInputOption='RAW',  
                body=request_body  
            ).execute()  
            
            logger.info(f"Data berhasil disimpan ke Google Sheets: {spreadsheet_id}")  
            return True  
        
        except Exception as e:  
            logger.error(f"Gagal menyimpan ke Google Sheets: {e}")  
            return False  

def load_data(  
    df: pd.DataFrame,  
    csv_path: Optional[str] = None,  
    postgresql_config: Optional[Dict[str, str]] = None,  
    google_sheets_config: Optional[Dict[str, str]] = None  
) -> Dict[str, bool]:  
    # Default path jika tidak disediakan  
    if csv_path is None:  
        csv_path = os.path.join(os.getcwd(), 'products.csv')  
    
    loader = DataLoader(csv_path)  
    
    result = {  
        'csv': False,  
        'postgresql': False,  
        'google_sheets': False  
    }  
    # Simpan ke CSV  
    result['csv'] = loader.save_to_csv(df)  
    
    # Simpan ke PostgreSQL jika konfigurasi tersedia  
    if postgresql_config:  
        result['postgresql'] = loader.save_to_postgresql(  
            df,  
            postgresql_config.get('connection_string', ''),  
            postgresql_config.get('table_name', 'fashion_products')  
        )  
    
    # Simpan ke Google Sheets jika konfigurasi tersedia  
    if google_sheets_config:  
        result['google_sheets'] = loader.save_to_google_sheets(  
            df,  
            google_sheets_config.get('spreadsheet_id', ''),  
            google_sheets_config.get('range_name', 'Sheet1!A1')  
        )  
    
    return result