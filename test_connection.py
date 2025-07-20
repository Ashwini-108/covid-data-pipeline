import mysql.connector
from mysql.connector import Error

def test_connection():
    try:
        print("Testing MySQL connection...")
        
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="mysql@123",
            database="covid_project"
        )
        
        if connection.is_connected():
            print("✓ Successfully connected to MySQL database")
            
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM covid_data")
            count = cursor.fetchone()[0]
            print(f"✓ Found {count} records in covid_data table")
            
            cursor.execute("SELECT countries, cases FROM covid_data LIMIT 3")
            records = cursor.fetchall()
            print("✓ Sample data:")
            for record in records:
                print(f"  {record[0]}: {record[1]:,} cases")
            
            cursor.close()
            connection.close()
            print("✓ Connection test successful!")
            
    except Error as e:
        print(f"✗ MySQL Error: {e}")
    except Exception as e:
        print(f"✗ General Error: {e}")

if __name__ == "__main__":
    test_connection()
    