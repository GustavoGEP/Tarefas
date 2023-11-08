import pandas as pd
import requests
import openpyxl
from bs4 import BeautifulSoup as soup

#Recolhimento dos dados

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4240.193 Safari/537.36" 
}

url = "https://www.theplasticsexchange.com/Research/WeeklyReview.aspx"

wb = openpyxl.load_workbook("INTL_TPE.xlsx")

sheet = wb.active

response = requests.get(url, headers=headers)

if response.status_code != 200:
    print("Error")

sopa = soup(response.content, "html.parser")

table = sopa.find("table", {"class" : "DataGrid"})

#Tratamento dos dados

columns = ["ID", "indicador"]

df_excel = pd.read_excel("INTL_TPE.xlsx", header = 3, usecols = columns)


df = pd.read_html(str(table))[0]

df.columns = df.loc[0]

df = df.drop(0, axis=0)

df = df.loc[:, ["Resin", "Bid"]]

df["Data"] = pd.to_datetime("2023-11-03")

df = df[["Resin","Data", "Bid"]]

df = df.rename(columns = {"Resin" : "indicador", "Bid" : "Valor"})


df_merged = df.merge(df_excel, on = "indicador", how = "left")

df = df_merged[["ID", "Data", "Valor"]]

df.dtypes

df["Valor"] = df["Valor"].str.replace("$", "")

df["Valor"] = df["Valor"].astype(float)

df.dtypes

a == 1