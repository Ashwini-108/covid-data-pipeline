import requests
import pandas as pd

def fetch_covid_data():
    url = "https://disease.sh/v3/covid-19/countries"
    response = requests.get(url)
    data = response.json()
    return pd.json_normalize(data)

def clean_data(df):
    df = df[['country', 'cases', 'deaths', 'recovered']].copy()
    df['mortality_rate'] = (df['deaths'] / df['cases']) * 100
    return df.sort_values(by='cases', ascending=False).head(10)