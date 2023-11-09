import requests 
import pandas as pd
import openpyxl
from bs4 import BeautifulSoup as soup
from requests import Session
from datetime import datetime, timedelta
import io
#Receber os dados

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/87.0.4240.193 Safari/537.36"
}

url = "https://www.theplasticsexchange.com/Research/WeeklyReview.aspx"

session = Session()

response = session.get(url, headers = headers)

sopa = soup(response.content, "html.parser")

datas = sopa.find("select", {"name": "ctl00$cphMain$ddlIssue"})
datas = datas.find_all("option")


lista = []
for data in datas:
    lista.append(data.text)

#data_inicio = "2022-10"

data_inicio = (datetime.today() - timedelta(days=397)).strftime("%Y-%m")

lista = pd.DataFrame(lista)

lista["Data"] = pd.to_datetime(lista[0])
lista.rename(columns = {0: "Site"}, inplace = True)

df_data = lista.loc[lista["Data"] >= data_inicio, "Site"]

df_lista = []

for index, row in df_data.items():

    response = session.get(url, headers = headers)

    sopa = soup(response.content, "html.parser")

    Script_Manager = "ctl00$cphMain$UpdatePanel1|ctl00$cphMain$ddlIssue"
    User_Name = ""
    Password = ""
    Main_Email = ""
    Event_Target = sopa.find("input", {"name": "__EVENTTARGET"}).get("value")
    Event_Argument = sopa.find("input", {"name": "__EVENTARGUMENT"}).get("value")
    Last_Focus = sopa.find("input", {"name": "__LASTFOCUS"}).get("value")
    View_State = sopa.find("input", {"name": "__VIEWSTATE"}).get("value")
    View_State_Generator = sopa.find("input", {"name": "__VIEWSTATEGENERATOR"}).get("value")
    Async_Post = "true"

    parametros = {
        "ctl00$cphMain$ScriptManager1" : Script_Manager,
        "ctl00$UserName" : User_Name,
        "ctl00$Password" : Password,
        "ctl00$cphMain$txtEmail" : Main_Email,
        "__EVENTTARGET" : Event_Target,
        "__EVENTARGUMENT" : Event_Argument,
        "__LASTFOCUS" : Last_Focus,
        "__VIEWSTATE" : View_State,
        "__VIEWSTATEGENERATOR" : View_State_Generator,
        "__ASYNCPOST" : Async_Post,
        "ctl00$cphMain$ddlIssue" : row
    }

    response = session.post(url, headers = headers, data = parametros)

    sopa = soup(response.content, "html.parser")

    table = sopa.find("table", {"class": "DataGrid"})

    table = io.StringIO(str(table))

    df = pd.read_html(table)[0]

    df.columns = df.loc[0]

    df = df.iloc[1:, :]

   
    df = df.loc[:, ["Resin", "Bid"]]

    
    df["Data"] = pd.to_datetime(row)

   
    df = df.rename(columns={"Resin": "indicador", "Bid": "Valor"})

    df_lista.append(df)



df = pd.concat(df_lista)

df.reset_index(drop=True, inplace=True)


wb = openpyxl.load_workbook("INTL_TPE.xlsx")

sheet = wb.active

columns = ["ID", "indicador"]
    
df_excel = pd.read_excel("INTL_TPE.xlsx", header = 3, usecols = columns)

df_merged = df.merge(df_excel, on="indicador", how="left")

df = df_merged[["ID", "Data", "Valor"]]

df["Valor"] = df["Valor"].str.replace("$", " ")


df["Valor"] = df["Valor"].astype(float)

dados_excel = pd.ExcelWriter("dados_excel.xlsx")

   
df.to_excel(dados_excel, sheet_name="dados_excel")

    
dados_excel.close()


