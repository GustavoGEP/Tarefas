from datetime import datetime, timedelta
from bs4 import BeautifulSoup as soup
import pandas as pd
import requests 

def receber_os_dados():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
        (KHTML, like Gecko) Chrome/87.0.4240.193 Safari/537.36"
    }
    url = "https://www.theplasticsexchange.com/Research/WeeklyReview.aspx"
    session = requests.Session()
    response = session.get(url, headers = headers)
    sopa = soup(response.content, "html.parser")
    datas = sopa.find("select", {"name": "ctl00$cphMain$ddlIssue"})
    datas = datas.find_all("option")
    return datas, session, headers, url

def lista_datas(datas:list):
    lista = []
    for data in datas:
        lista.append(data.text)
    data_inicio = (datetime.today() - timedelta(days=397)).strftime("%Y-%m")
    lista = pd.DataFrame(lista)
    lista["Data"] = pd.to_datetime(lista[0])
    lista.rename(columns = {0: "Site"}, inplace = True)
    df_data = lista.loc[lista["Data"] >= data_inicio]
    return df_data

def recolhimento_dos_dados(df_data:pd.DataFrame, session, url, headers):
    df_lista = []
    for index, row in df_data.iterrows():
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
            "ctl00$cphMain$ddlIssue" : row['Site']
        }
        response = session.post(url, headers = headers, data = parametros)
        sopa = soup(response.content, "html.parser")
        table = sopa.find("table", {"class": "DataGrid"})
        df = pd.read_html(table.prettify())[0]
        df.columns = df.loc[0]
        df = df.loc[1:, ["Resin", "Bid"]]
        df["Data"] = row['Data']
        df = df.rename(columns={"Resin": "indicador", "Bid": "Valor"})
        df_lista.append(df)
    return df_lista   


def tratamento_dos_dados(df_lista:pd.DataFrame):
    df = pd.concat(df_lista)
    df.reset_index(drop=True, inplace=True)
    columns = ["ID", "indicador"]
    df_excel = pd.read_excel("INTL_TPE.xlsx", header = 3, usecols = columns)
    df_merged = df.merge(df_excel, on="indicador", how="left")
    df = df_merged[["ID", "Data", "Valor"]]
    df["Valor"] = df["Valor"].str.replace("$", " ")
    df["Valor"] = df["Valor"].astype(float)
    return df


def baixar_excel(df:pd.DataFrame):
    df.to_excel('dados_excel2.xlsx', sheet_name="dados_excel2")

def main():
    dados, session, headers, url = receber_os_dados()
    df_data= lista_datas(dados)
    df_lista = recolhimento_dos_dados(df_data, session, url, headers)
    df = tratamento_dos_dados(df_lista)
    baixar_excel(df)    

if __name__ == "__main__":
    main()