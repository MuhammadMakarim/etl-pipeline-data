import logging  
import re  
from typing import Optional  
import pandas as pd  
import numpy as np  

# Konfigurasi Logging  
logging.basicConfig(  
    level=logging.INFO,   
    format='%(asctime)s - %(levelname)s: %(message)s'  
)  
logger = logging.getLogger(__name__)  

class DataTransformer:  
    @staticmethod  
    def _clean_rating(rating: str) -> Optional[float]:  
        try:  
            match = re.search(r'(\d+(?:\.\d+)?)', str(rating))  
            return float(match.group(1)) if match else None  
        except Exception as e:  
            logger.warning(f"Error ekstraksi rating: {e}")  
            return None  

    @staticmethod  
    def _clean_price(price: str) -> Optional[float]:  
        try:  
            # Ekstraksi angka dari teks harga  
            price_match = re.search(r'\$?(\d+(?:\.\d+)?)', str(price))  
            
            if not price_match:  
                return None  
            
            # Konversi ke float dan ke Rupiah  
            price_usd = float(price_match.group(1))  
            price_idr = price_usd * 16000  
            
            return round(price_idr, 2)  
        
        except Exception as e:  
            logger.warning(f"Error konversi harga: {e}")  
            return None  

    @staticmethod  
    def _clean_colors(colors: str) -> Optional[int]:  
        try:  
            match = re.search(r'(\d+)', str(colors))  
            return int(match.group(1)) if match else None  
        except Exception as e:  
            logger.warning(f"Error ekstraksi warna: {e}")  
            return None  

    @staticmethod  
    def _normalize_size(size: str) -> str:  
        size_map = {  
            's': 'S', 'm': 'M', 'l': 'L',   
            'xl': 'XL', 'xxl': 'XXL',  
            'small': 'S', 'medium': 'M',   
            'large': 'L', 'xlarge': 'XL'  
        }  
        
        # Hapus prefix 'Size: ' jika ada  
        clean_size = str(size).lower().replace('size:', '').strip()  
        
        return size_map.get(clean_size, clean_size.upper())  

    @staticmethod  
    def _normalize_gender(gender: str) -> str:  
        gender_map = {  
            'male': 'Men', 'men': 'Men',  
            'female': 'Women', 'women': 'Women',  
            'unisex': 'Unisex'  
        }  
        
        # Hapus prefix 'Gender: ' jika ada  
        clean_gender = str(gender).lower().replace('gender:', '').strip()  
        
        return gender_map.get(clean_gender, 'Unknown')  

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:   
        try:  
            # Validasi input  
            if df is None or df.empty:  
                logger.warning("DataFrame kosong atau None")  
                return pd.DataFrame()  
            
            # Buat salinan DataFrame  
            transformed_df = df.copy()  
            
            # Pembersihan dan transformasi kolom  
            transformed_df['Rating'] = transformed_df['Rating'].apply(self._clean_rating)  
            transformed_df['Price'] = transformed_df['Price'].apply(self._clean_price)  
            transformed_df['Colors'] = transformed_df['Colors'].apply(self._clean_colors)  
            transformed_df['Size'] = transformed_df['Size'].apply(self._normalize_size)  
            transformed_df['Gender'] = transformed_df['Gender'].apply(self._normalize_gender)  
            
            # Hapus baris dengan data tidak valid  
            transformed_df.dropna(  
                subset=['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender'],   
                inplace=True  
            )  
            
            # Tambahkan kolom timestamp  
            transformed_df['Timestamp'] = pd.Timestamp.now().isoformat()  
            
            # Hapus duplikat  
            transformed_df.drop_duplicates(inplace=True)  
            
            logger.info(f"Transformasi data berhasil. Jumlah data: {len(transformed_df)}")  
            return transformed_df  
        
        except Exception as e:  
            logger.error(f"Gagal melakukan transformasi: {e}")  
            return pd.DataFrame()  

def transform_data(df: pd.DataFrame) -> pd.DataFrame:  
    # Validasi input  
    if not isinstance(df, pd.DataFrame):  
        raise TypeError("Input harus berupa DataFrame")  
    
    if df.empty:  
        return pd.DataFrame()  
    
    transformer = DataTransformer()  
    return transformer.transform(df)  