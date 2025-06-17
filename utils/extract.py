import logging  
import re  
from typing import List, Dict, Optional  
from datetime import datetime  

import requests  
from bs4 import BeautifulSoup  
import pandas as pd  

# Konfigurasi Logging  
logging.basicConfig(  
    level=logging.INFO,   
    format='%(asctime)s - %(levelname)s: %(message)s'  
)  
logger = logging.getLogger(__name__)  

class DataExtractor:  
    def __init__(  
        self,   
        base_url: str = 'https://fashion-studio.dicoding.dev/',   
        max_pages: int = 50  
    ):  
        self.base_url = base_url  
        self.max_pages = max_pages  
        self.session = requests.Session()  
        self.session.headers.update({  
            'User-Agent': 'Mozilla/5.0 ETL Pipeline Scraper'  
        })  

    def _parse_price(self, price_text: str) -> Optional[float]:  
        try:  
            # Tangani "Price Unavailable"  
            if price_text == "Price Unavailable":  
                return None  
            
            # Ekstrak angka dengan regex  
            price_match = re.search(r'\$?(\d+(?:\.\d+)?)', price_text)  
            return float(price_match.group(1)) if price_match else None  
        
        except Exception as e:  
            logger.error(f"Error parsing price: {e}")  
            return None  

    def _extract_text(  
        self,   
        element: Optional[BeautifulSoup],   
        default: Optional[str] = None  
    ) -> Optional[str]:  
        try:  
            return element.get_text(strip=True) if element else default  
        except Exception as e:  
            logger.warning(f"Error extracting text: {e}")  
            return default  

    def _extract_rating(self, rating_text: str) -> Optional[float]:  
        try:  
            if not rating_text or "Invalid Rating" in rating_text:  
                return None  
            
            match = re.search(r'(\d+(?:\.\d+)?)', rating_text)  
            return float(match.group(1)) if match else None  
        
        except Exception as e:  
            logger.warning(f"Error extracting rating: {e}")  
            return None  

    def _extract_colors(self, colors_text: str) -> int:  
        try:  
            match = re.search(r'(\d+)', colors_text or '')  
            return int(match.group(1)) if match else 0  
        
        except Exception as e:  
            logger.warning(f"Error extracting colors: {e}")  
            return 0  

    def extract(  
        self,   
        base_url: str = 'https://fashion-studio.dicoding.dev/',  
        max_pages: int = 50,  
        max_items: int = 1000  
    ) -> pd.DataFrame:  
        """  
        Metode utama untuk ekstraksi data dengan pembatasan jumlah items  
        """  
        products = self.scrape_products()  
        
        # Batasi jumlah produk sesuai max_items  
        products = products[:max_items]  
        
        return pd.DataFrame(products)  

    def scrape_products(self) -> List[Dict]:   
        products = []  
        
        try:  
            for page in range(1, self.max_pages + 1):  
                try:  
                    # Penyesuaian URL untuk halaman pertama  
                    url = (  
                        self.base_url if page == 1   
                        else f'{self.base_url}page{page}'  
                    )  
                    
                    response = self.session.get(url)  
                    response.raise_for_status()  
                    
                    soup = BeautifulSoup(response.content, 'html.parser')  
                    
                    # Temukan semua kartu produk  
                    cards = soup.select('.collection-card')  
                    
                    for card in cards:  
                        try:  
                            # Ekstraksi title  
                            title_elem = card.select_one('.product-title')  
                            title = self._extract_text(title_elem, 'Unknown Product')  
                            
                            # Ekstraksi price  
                            price_elem = card.select_one('.price, .price-container .price')  
                            price_text = self._extract_text(price_elem, 'Price Unavailable')  
                            price = self._parse_price(price_text)  
                            
                            # Skip jika price invalid  
                            if price is None or title == "Unknown Product":  
                                continue  
                            
                            # Ekstraksi detail lainnya  
                            details = card.select('p')  
                            
                            rating_text = self._extract_text(  
                                details[0] if details and 'Rating' in details[0].text   
                                else None  
                            )  
                            rating = self._extract_rating(rating_text)  
                            
                            colors_text = self._extract_text(  
                                details[1] if len(details) > 1 and 'Colors' in details[1].text   
                                else None  
                            )  
                            colors = self._extract_colors(colors_text)  
                            
                            size_text = self._extract_text(  
                                details[2] if len(details) > 2 and 'Size:' in details[2].text  
                                else None,   
                                'Unknown'  
                            )  
                            size = size_text.replace('Size: ', '') if size_text else None  
                            
                            gender_text = self._extract_text(  
                                details[3] if len(details) > 3 and 'Gender:' in details[3].text  
                                else None,   
                                'Unknown'  
                            )  
                            gender = gender_text.replace('Gender: ', '') if gender_text else None  
                            
                            product = {  
                                'Title': title,  
                                'Price': price,  
                                'Rating': rating,  
                                'Colors': colors,  
                                'Size': size,  
                                'Gender': gender,  
                                'timestamp': datetime.now().isoformat()  
                            }  
                            
                            products.append(product)  
                        
                        except Exception as item_error:  
                            logger.error(f"Error processing item: {item_error}")  
                
                except requests.exceptions.RequestException as page_error:  
                    logger.error(f"Error fetching page {page}: {page_error}")  
                    continue  
            
            logger.info(f"Berhasil mengekstrak {len(products)} produk")  
            return products  
        
        except Exception as e:  
            logger.critical(f"Fatal error dalam scraping: {e}")  
            return []  

# Fungsi wrapper untuk memudahkan pemanggilan  
def extract_data():  
    extractor = DataExtractor()  
    return extractor.extract()  