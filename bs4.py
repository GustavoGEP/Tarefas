import pandas as pd
import requests 
from bs4 import BeautifulSoup as soup

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    "AppleWebKit/537.36 (KHTML, like Gecko)"
    "Chrome/70.0.3538.77 Safari/537.36"
}

url = "https://finance.yahoo.com/quote/JDEP.AS/history?p=JDEP.AS"

response = requests.get(url, headers=headers)

if response.status_code != 200:
    raise Exception("Erro no request")

sopa = soup(response.text, "html.parser")

table = sopa.find("table", {"data-test": "historical-prices"})



df = pd.read_html(table.prettify())[0]

df["ID"] = "JDEP.AS"

df = df[["ID","Date","Close*"]]

df = df.iloc[:-1]

df["Date"] = pd.to_datetime(df["Date"], format = "%b %d, %Y")

df["Close*"] = df["Close*"].apply(lambda x: x.split(" ")[0])

df = df.astype({"Close*" : float})

df = df.rename(columns = {"Close*" : "Valor", "Date" : "Data"})