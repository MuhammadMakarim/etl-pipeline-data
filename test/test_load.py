import pytest  
import pandas as pd  
from unittest.mock import patch, MagicMock  
import sys  
import os  

# Menambahkan path agar bisa import utils  
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  

from utils.load import DataLoader  

# Fixture DataFrame  
@pytest.fixture  
def sample_dataframe():  
    return pd.DataFrame({  
        "Title": ["Item A"],  
        "Price": [160000.0],  
        "Rating": [4.5],  
        "Colors": [3],  
        "Size": ["M"],  
        "Gender": ["Male"],  
        "Timestamp": ["2025-05-10T10:00:00.000000"]  
    })  

class TestLoadFunctions:  
    def test_save_to_csv(self, sample_dataframe, tmp_path):  
        # Arrange  
        csv_path = tmp_path / "test_fashion.csv"  
        loader = DataLoader(csv_path=str(csv_path))  
        
        # Act  
        result = loader.save_to_csv(sample_dataframe)  
        
        # Assert  
        assert result is True  
        assert os.path.exists(csv_path)  
        
        # Validasi isi CSV  
        df_read = pd.read_csv(csv_path)  
        assert not df_read.empty  
        assert list(df_read.columns) == list(sample_dataframe.columns)  

    def test_save_to_csv_empty_dataframe(self):  
        # Arrange  
        loader = DataLoader()  
        empty_df = pd.DataFrame()  
        
        # Act & Assert  
        result = loader.save_to_csv(empty_df)  
        assert result is False  

    @patch('utils.load.create_engine')  
    def test_save_to_postgresql(self, mock_create_engine, sample_dataframe):  
        # Arrange  
        # Buat mock engine yang sesuai dengan persyaratan SQLAlchemy  
        mock_engine = MagicMock()  
        mock_create_engine.return_value = mock_engine  
        
        # Patch method to_sql pada DataFrame  
        with patch.object(sample_dataframe, 'to_sql') as mock_to_sql:  
            loader = DataLoader()  
            connection_string = 'postgresql://developer:your_pass@localhost:5432/fashion_db'  
            table_name = 'fashion_products'  
            
            # Act  
            result = loader.save_to_postgresql(  
                sample_dataframe,  
                connection_string,  
                table_name  
            )  
            
            # Assert  
            # Pastikan create_engine dipanggil dengan connection string  
            mock_create_engine.assert_called_once_with(connection_string)  
            
            # Validasi pemanggilan to_sql dengan parameter yang benar  
            mock_to_sql.assert_called_once_with(  
                name=table_name,  
                con=mock_engine,  
                if_exists='replace',  
                index=False  
            )  
            
            # Pastikan hasilnya True  
            assert result is True  

    @patch('utils.load.create_engine', side_effect=Exception("Connection Error"))  
    def test_save_to_postgresql_connection_error(self, mock_create_engine, sample_dataframe):  
        # Arrange  
        loader = DataLoader()  
        connection_string = 'postgresql://developer:your_pass@localhost:5432/fashion_db'  
        table_name = 'fashion_products'  
        
        # Act  
        result = loader.save_to_postgresql(  
            sample_dataframe,   
            connection_string,   
            table_name  
        )  
        
        # Assert  
        assert result is False  

    @patch('utils.load.service_account.Credentials.from_service_account_file')  
    @patch('utils.load.build')  
    def test_save_to_google_sheets(self, mock_build, mock_creds, sample_dataframe):  
        # Arrange  
        mock_service = MagicMock()  
        mock_build.return_value = mock_service  
        mock_creds.return_value = MagicMock()  
        
        loader = DataLoader()  
        spreadsheet_id = 'test_spreadsheet_id'  
        range_name = 'Sheet1!A1'  
        
        # Act  
        result = loader.save_to_google_sheets(  
            sample_dataframe,   
            spreadsheet_id,   
            range_name  
        )  
        
        # Assert  
        assert result is True  
        mock_build.assert_called_once()  

    def test_save_to_google_sheets_empty_dataframe(self):  
        # Arrange  
        loader = DataLoader()  
        empty_df = pd.DataFrame()  
        
        # Act & Assert  
        result = loader.save_to_google_sheets(empty_df, 'test_id', 'Sheet1!A1')  
        assert result is False
