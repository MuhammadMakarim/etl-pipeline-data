import sys  
import os  
import unittest  
import pandas as pd  
import numpy as np  
from unittest.mock import patch  

# Tambahkan path parent directory  
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  

from utils.transform import transform_data, DataTransformer  

class TestTransformFunctions(unittest.TestCase):  
    def setUp(self):  
        self.sample_fashion_data = pd.DataFrame({  
            'Title': ['Trendy Shirt', 'Unknown Product', 'Stylish Jacket'],  
            'Price': ['$50.25', 'Unavailable', '$75.50'],  
            'Rating': ['4.5/5', 'Invalid Rating', '3.8/5'],  
            'Colors': ['3 Colors', '2 Colors', '4 Colors'],  
            'Size': ['Size: M', 'Size: Unknown', 'Size: L'],  
            'Gender': ['Gender: Men', 'Gender: Unknown', 'Gender: Unisex']  
        })  

    def test_successful_data_transformation(self):  
        transformed_data = transform_data(self.sample_fashion_data)  
        
        # Validasi struktur data  
        self.assertIsInstance(transformed_data, pd.DataFrame)  
        self.assertEqual(len(transformed_data), 2)  
        
        # Validasi tipe data  
        self.assertEqual(transformed_data['Price'].dtype, np.float64)  
        self.assertEqual(transformed_data['Rating'].dtype, np.float64)  
        self.assertEqual(transformed_data['Colors'].dtype, np.int64)  
        
        # Validasi konversi data  
        self.assertAlmostEqual(transformed_data.iloc[0]['Price'], 804000.0)  # $50.25 * 16000  
        self.assertAlmostEqual(transformed_data.iloc[0]['Rating'], 4.5)  
        self.assertEqual(transformed_data.iloc[0]['Colors'], 3)  
        self.assertEqual(transformed_data.iloc[0]['Size'], 'M')  
        self.assertEqual(transformed_data.iloc[0]['Gender'], 'Men')  

    def test_data_cleaning_rules(self):  
        cleaned_data = transform_data(self.sample_fashion_data)  
        
        # Pastikan tidak ada data 'unknown' atau 'invalid'  
        self.assertFalse(cleaned_data['Title'].str.contains('Unknown', case=False).any())  
        self.assertFalse(cleaned_data['Rating'].astype(str).str.contains('Invalid', case=False).any())  

    def test_error_handling(self):  
        # DataFrame kosong  
        result = transform_data(pd.DataFrame())  
        self.assertTrue(result.empty)  

        # Input bukan DataFrame  
        with self.assertRaises(TypeError):  
            transform_data("Invalid Input")  

    @patch('utils.transform.pd.Timestamp.now')  
    def test_timestamp_generation(self, mock_now):  
        mock_timestamp = '2025-02-10T13:54:32.640365'  
        mock_now.return_value = pd.Timestamp(mock_timestamp)  
        
        transformed_data = transform_data(self.sample_fashion_data)  
        
        # Validasi kolom timestamp  
        self.assertIn('Timestamp', transformed_data.columns)  
        self.assertTrue(transformed_data['Timestamp'].str.contains('T').all())  
        self.assertEqual(transformed_data.iloc[0]['Timestamp'], mock_timestamp)  

    def test_data_validation_rules(self):  
        transformed_data = transform_data(self.sample_fashion_data)  
        
        # Validasi rentang harga  
        self.assertTrue(all(transformed_data['Price'] > 0))  
        
        # Validasi rentang rating  
        self.assertTrue(all((transformed_data['Rating'] >= 0) & (transformed_data['Rating'] <= 5)))  
        
        # Validasi jumlah warna  
        self.assertTrue(all(transformed_data['Colors'] > 0))  

if __name__ == '__main__':  
    unittest.main() 