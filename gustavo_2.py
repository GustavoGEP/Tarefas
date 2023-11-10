import requests
import pandas as pd
import openpyxl
from bs4 import BeautifulSoup as soup
from requests import Session
from datetime import datetime, timedelta
import io

class ColetorEProcessadorDados:
    def __init__(self):
        self.url = "https://www.theplasticsexchange.com/Research/WeeklyReview.aspx"
        self.headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4240.193 Safari/537.36"
        }
        self.session = Session()

    def __receber_as_datas(self):
        response = self.session.get(self.url, headers=self.headers)
        sopa = soup(response.content, "html.parser")
        datas = sopa.find("select", {"name": "ctl00$cphMain$ddlIssue"}).find_all("option")
        return datas

    def __lista_datas(self):
        datas = self.__receber_as_datas()
        lista = [data.text for data in datas]
        data_inicio = (datetime.today() - timedelta(days=397)).strftime("%Y-%m")
        df_data = pd.DataFrame(lista, columns=["Site"])
        df_data["Data"] = pd.to_datetime(df_data["Site"])
        df_data = df_data.loc[df_data["Data"] >= data_inicio, "Site"]
        return df_data

    def recolhimento_dos_dados(self):
        df_lista = []
        df_data = self.__lista_datas()
        for row in df_data:
            response = self.session.get(self.url, headers=self.headers)
            sopa = soup(response.content, "html.parser")
            Script_Manager = "ctl00$cphMain$UpdatePanel1|ctl00$cphMain$ddlIssue"
            User_Name, Password, Main_Email = "", "", ""
            Event_Target = sopa.find("input", {"name": "__EVENTTARGET"}).get("value")
            Event_Argument = sopa.find("input", {"name": "__EVENTARGUMENT"}).get("value")
            Last_Focus = sopa.find("input", {"name": "__LASTFOCUS"}).get("value")
            View_State = sopa.find("input", {"name": "__VIEWSTATE"}).get("value")
            View_State_Generator = sopa.find("input", {"name": "__VIEWSTATEGENERATOR"}).get("value")
            Async_Post = "true"
            parametros = {
                "ctl00$cphMain$ScriptManager1": Script_Manager,
                "ctl00$UserName": User_Name,
                "ctl00$Password": Password,
                "ctl00$cphMain$txtEmail": Main_Email,
                "__EVENTTARGET": Event_Target,
                "__EVENTARGUMENT": Event_Argument,
                "__LASTFOCUS": Last_Focus,
                "__VIEWSTATE": View_State,
                "__VIEWSTATEGENERATOR": View_State_Generator,
                "__ASYNCPOST": Async_Post,
                "ctl00$cphMain$ddlIssue": row
            }
            response = self.session.post(self.url, headers=self.headers, data=parametros)
            sopa = soup(response.content, "html.parser")
            table = sopa.find("table", {"class": "DataGrid"})
            table = io.StringIO(str(table))
            df = pd.read_html(table)[0]
            df.columns = df.loc[0]
            df = df.iloc[1:, :]
            df = df[["Resin", "Bid"]]  
            df["Data"] = pd.to_datetime(row)
            df = df.rename(columns={"Resin": "indicador", "Bid": "Valor"})
            df_lista.append(df)
        self.df_lista = df_lista

    def tratamento_dos_dados(self):
        df = pd.concat(self.df_lista)
        df.reset_index(drop=True, inplace=True)
        columns = ["ID", "indicador"]
        df_excel = pd.read_excel("INTL_TPE.xlsx", header=3, usecols=columns)
        df_merged = df.merge(df_excel, on="indicador", how="left")
        df = df_merged[["ID", "Data", "Valor"]]
        df["Valor"] = df["Valor"].str.replace("$", " ")
        df["Valor"] = df["Valor"].astype(float)
        self.df = df

    def baixar_excel(self):
        self.df.to_excel("dados_excel2.xlsx", sheet_name="dados_excel2", index=False)


def main():
    bot = ColetorEProcessadorDados()
    bot.recolhimento_dos_dados()
    bot.tratamento_dos_dados()
    bot.baixar_excel()


if __name__ == "__main__":
    main()