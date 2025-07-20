import mysql.connector
import pandas as pd
from mysql.connector import Error
import time

def fetch_covid_data():
    """Fetch COVID-19 data from MySQL database instead of API"""
    connection = None
    try:
        print("Attempting to connect to MySQL database...")
        
        # Database connection with explicit parameters
        connection = mysql.connector.connect(
            host="localhost",
            port=3306,
            user="root",
            password="mysql@123",
            database="covid_project",
            connection_timeout=10,
            autocommit=True,
            use_pure=True  # Force pure Python implementation
        )
        
        print("Connection object created, checking status...")
        
        if connection.is_connected():
            print("Successfully connected to MySQL database")
            
            # Get database info
            db_info = connection.get_server_info()
            print(f"Server version: {db_info}")
            
            # Create cursor
            cursor = connection.cursor(buffered=True)
            
            # Test basic query first
            print("Testing basic connection...")
            cursor.execute("SELECT 1")
            test_result = cursor.fetchone()
            print(f"Basic test result: {test_result}")
            
            # Check if table exists
            print("Checking if covid_data table exists...")
            cursor.execute("SHOW TABLES LIKE 'covid_data'")
            table_exists = cursor.fetchone()
            
            if not table_exists:
                print("Error: Table 'covid_data' does not exist")
                return pd.DataFrame()
            
            print("Table exists, counting records...")
            cursor.execute("SELECT COUNT(*) FROM covid_data")
            row_count = cursor.fetchone()[0]
            print(f"Found {row_count} records in covid_data table")
            
            if row_count == 0:
                print("Warning: covid_data table is empty")
                return pd.DataFrame()
            
            # Fetch the actual data
            print("Fetching data...")
            query = """
            SELECT countries as country, cases, deaths, mortality_rate 
            FROM covid_data 
            ORDER BY cases DESC
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Convert to DataFrame manually
            columns = ['country', 'cases', 'deaths', 'mortality_rate']
            df = pd.DataFrame(results, columns=columns)
            
            print(f"Successfully fetched {len(df)} records from database")
            
            cursor.close()
            return df
            
        else:
            print("Failed to establish connection")
            return pd.DataFrame()
            
    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
        print(f"Error Number: {err.errno}")
        if hasattr(err, 'sqlstate'):
            print(f"SQL State: {err.sqlstate}")
        return pd.DataFrame()
        
    except Exception as e:
        print(f"General Error: {e}")
        print(f"Error Type: {type(e).__name__}")
        return pd.DataFrame()
        
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("MySQL connection closed")

def clean_data(df):
    """Clean and process the fetched data"""
    if df.empty:
        print("No data to process")
        return pd.DataFrame()
    
    # The data is already cleaned from database, just sort and limit
    df = df.sort_values(by='cases', ascending=False).head(10)
    return df