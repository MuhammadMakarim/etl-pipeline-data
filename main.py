import logging  
import os  
import pandas as pd  
from datetime import datetime   

from utils.extract import DataExtractor  
from utils.transform import DataTransformer  
from utils.load import DataLoader, load_data  

# Konfigurasi Logging  
logging.basicConfig(  
    level=logging.INFO,   
    format='%(asctime)s - %(levelname)s: %(message)s'  
)  
logger = logging.getLogger(__name__)  

class ETLPipeline:  
    def __init__(  
        self,   
        base_url: str = 'https://fashion-studio.dicoding.dev',  
        max_pages: int = 50,  
        max_items: int = 1000  
    ):  
        self.base_url = base_url  
        self.max_pages = max_pages  
        self.max_items = max_items  
        
        # Inisialisasi komponen ETL  
        self.extractor = DataExtractor()  
        self.transformer = DataTransformer()  
        self.loader = DataLoader()  

    def run(self) -> dict:  
        try:  
            # Pastikan direktori proyek ada  
            project_dir = os.path.dirname(os.path.abspath(__file__))  
            csv_path = os.path.join(project_dir, 'products.csv')  
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)  
            
            # Pastikan kredensial Google Sheets ada  
            google_credentials_path = os.path.join(project_dir, 'google-sheets-api.json')  
            if not os.path.exists(google_credentials_path):  
                logger.error("File kredensial Google Sheets tidak ditemukan!")  
                return {}  

            # 1. Ekstraksi Data  
            logger.info("Memulai proses ekstraksi data...")  
            raw_df = self.extractor.extract(  
                base_url=self.base_url,   
                max_pages=self.max_pages,   
                max_items=self.max_items  
            )  
            
            if raw_df is None or raw_df.empty:  
                logger.warning("Tidak ada data yang berhasil diekstraksi")  
                return {}  
            
            logger.info(f"Jumlah data terekstraksi: {len(raw_df)}")  
            
            # 2. Transformasi Data  
            logger.info("Memulai proses transformasi data...")  
            cleaned_df = self.transformer.transform(raw_df)  
            
            if cleaned_df is None or cleaned_df.empty:  
                logger.warning("Tidak ada data yang berhasil ditransformasi")  
                return {}  
            
            logger.info(f"Jumlah data setelah transformasi: {len(cleaned_df)}")  
            
            # 3. Load Data  
            logger.info("Memulai proses pemuatan data...")  
            load_result = load_data(  
                df=cleaned_df,  
                csv_path=os.path.join(project_dir, 'products.csv'),  
                postgresql_config={  
                    'connection_string': 'postgresql://developer:your_pass@localhost:5432/fashion_db',  
                    'table_name': 'fashion_products'  
                },  
                google_sheets_config={  
                    'spreadsheet_id': '1zc5SKT_Q9vCzuHB0TTURzIc8qbZd4Om6qaSlC8kAAjg',  
                    'range_name': 'Sheet1!A1'  
                }  
            )  
            
            # Hitung ringkasan proses  
            success_count = sum(load_result.values())  
            total_count = len(load_result)  
            success_rate = (success_count / total_count) * 100 if total_count > 0 else 0  
            
            logger.info("\nRingkasan Proses:")  
            logger.info(f"Total operasi    : {total_count}")  
            logger.info(f"Berhasil         : {success_count}")  
            logger.info(f"Gagal            : {total_count - success_count}")  
            logger.info(f"Tingkat sukses   : {success_rate:.1f}%")  
            
            return load_result  
        
        except Exception as e:  
            logger.error(f"Kesalahan pada proses ETL: {e}")  
            return {}  

def main():  
    # Pastikan direktori proyek ada  
    project_dir = os.path.dirname(os.path.abspath(__file__))  
    csv_path = os.path.join(project_dir, 'products.csv')  
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)  
    
    pipeline = ETLPipeline()  
    pipeline.run()  

if __name__ == '__main__':  
    main()  
