import sys  
import os  
import unittest  
import pandas as pd  
from unittest.mock import patch, MagicMock  

# Tambahkan path parent directory  
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  

from utils.extract import extract_data, DataExtractor  

class TestExtractFunctions(unittest.TestCase):  
    @patch('utils.extract.DataExtractor.scrape_products')   
    def test_extract_successful_single_product(self, mock_scrape_products):  
        mock_products = [
            {
                'Title': 'Test Fashion Product',  
                'Price': 100.00,  
                'Rating': 4.5,  
                'Colors': 3,  
                'Size': 'M',  
                'Gender': 'Men',  
                'timestamp': '2023-01-01T00:00:00' 
            }  
        ]
        # Set mock return value  
        mock_scrape_products.return_value = mock_products  

        extractor = DataExtractor(max_pages=1)  
        df = extractor.extract()  
    
        # Validasi  
        self.assertIsInstance(df, pd.DataFrame)  
        self.assertEqual(len(df), 1)  
        self.assertEqual(df.iloc[0]['Title'], 'Test Fashion Product')


    @patch('utils.extract.DataExtractor.scrape_products')  
    def test_extract_multiple_products(self, mock_scrape_products):  
        mock_products = [
            {
                'Title': 'Product 1',  
                'Price': 50.00,  
                'Rating': 4.0,  
                'Colors': 2,  
                'Size': 'S',  
                'Gender': 'Women',  
                'timestamp': '2023-01-01T00:00:00'
            },
            {
                'Title': 'Product 2',  
                'Price': 75.00,  
                'Rating': 4.7,  
                'Colors': 3,  
                'Size': 'L',  
                'Gender': 'Unisex',  
                'timestamp': '2023-01-01T00:00:00'  
            }
        ]

         # Set mock return value  
        mock_scrape_products.return_value = mock_products  

        extractor = DataExtractor(max_pages=1)  
        df = extractor.extract()  
    
        # Validasi  
        self.assertIsInstance(df, pd.DataFrame)  
        self.assertEqual(len(df), 2)  
        self.assertEqual(df.iloc[0]['Title'], 'Product 1')  
        self.assertEqual(df.iloc[1]['Title'], 'Product 2')

    @patch('utils.extract.requests.Session.get')  
    def test_extract_handles_invalid_data(self, mock_get):  
        """Uji penanganan data tidak valid"""  
        mock_response = MagicMock()  
        mock_response.status_code = 200  
        mock_response.content = '''  
        <div class="collection-card">  
            <h3 class="product-title">Invalid Product</h3>  
            <div class="price-container">Invalid Price</div>  
            <p>Rating: Invalid Rating</p>  
            <p>Colors: Invalid Colors</p>  
            <p>Size: Invalid Size</p>  
            <p>Gender: Invalid Gender</p>  
        </div>  
        '''  
        mock_get.return_value = mock_response  

        extractor = DataExtractor(max_pages=1)  
        df = extractor.extract()  
        
        self.assertIsInstance(df, pd.DataFrame)  
        self.assertEqual(len(df), 0)  

    def test_extract_with_network_error(self):  
        """Uji penanganan kesalahan jaringan"""  
        with patch('utils.extract.requests.Session.get', side_effect=Exception('Network Error')):  
            extractor = DataExtractor(max_pages=1)  
            df = extractor.extract()  
            
            self.assertIsInstance(df, pd.DataFrame)  
            self.assertTrue(df.empty)  

if __name__ == '__main__':  
    unittest.main()  