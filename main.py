from covid_api import fetch_covid_data, clean_data

def main():
    print("Fetching COVID-19 data from MySQL database...")
    raw_data = fetch_covid_data()
    
    if raw_data.empty:
        print("No data retrieved from database. Please check your connection and data.")
        return
    
    cleaned_data = clean_data(raw_data)
    
    if cleaned_data.empty:
        print("No data to display after cleaning.")
        return
    
    print("\nTOP 10 COUNTRIES BY COVID CASES")
    print("=" * 60)
    
    for i, row in cleaned_data.iterrows():
        print(f"{row['country']:<12} | {row['cases']:>12,} cases | {row['mortality_rate']:>5.1f}% mortality")
    
    cleaned_data.to_csv('covid_data.csv', index=False)
    print(f"\nData saved to 'covid_data.csv'")

if __name__ == "__main__":
    main()