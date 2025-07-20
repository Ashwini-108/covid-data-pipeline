import mysql.connector
import pandas as pd

try:
    print("📡 Connecting to MySQL...")

    # Connect to MySQL and covid_project database
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="mysql@123",
        database="covid_project"
    )

    print("✅ Connected to 'covid_project' database.")

    # Create a cursor
    cursor = conn.cursor()

    # Check if covid_data table exists
    cursor.execute("SHOW TABLES LIKE 'covid_data'")
    table_result = cursor.fetchone()

    if table_result:
        print("✅ 'covid_data' table found.")

        # Count rows
        cursor.execute("SELECT COUNT(*) FROM covid_data")
        count = cursor.fetchone()[0]
        print(f"📊 Total rows in 'covid_data': {count}")

        if count > 0:
            # Fetch and display top 10 rows
            df = pd.read_sql("SELECT * FROM covid_data LIMIT 10", conn)
            print("\n📝 First 10 rows:")
            print(df)
        else:
            print("⚠️ Table 'covid_data' is empty.")
    else:
        print("❌ 'covid_data' table not found in the database.")

    # Close connection
    conn.close()
    print("🔒 Connection closed.")

except mysql.connector.Error as err:
    print("❌ MySQL Error:", err)

except Exception as e:
    print("❌ General Error:", e)
