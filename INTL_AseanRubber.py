import requests
import pandas as pd
from bs4 import BeautifulSoup as soup
from datetime import datetime
import openpyxl
import numpy as np

class INTL_AseanRubber:
    def __init__(self):
        self.df = pd.DataFrame()
        self.endpoint = "https://www.aseanrubber.net/arbc/"
        self.collected = []
    
    def coleta(self):
        request = requests.get(self.endpoint)
        sopa = soup(request.content, "html.parser")
        meses = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"]
        lis = [li for li in sopa.findAll("a", href=True) if any(mes in li.text.lower() for mes in meses)]
        self.endpoint = "https://www.aseanrubber.net"
        for li in lis:
            url = "{}{}".format(self.endpoint, li.get("href"))
            data_da_url = url.split("/")[-1]
            if "various-market" in url:
                if "2018" in url:
                    mes_ano = data_da_url.lower().replace("-", "")
                else:
                    mes_ano = "{}{}".format(data_da_url.lower().replace("-", ""), "2018")
            elif "2019-prices" in url:
                if "may-2" in url:
                    mes_ano = "may2019"
                elif "sept2019" in url:
                    mes_ano = "september2019"
                elif not "2019" in data_da_url:
                    mes_ano = "{}{}".format(data_da_url.lower().replace("-", ""), "2019")
                else:
                    mes_ano = data_da_url.lower().replace("-", "")
            elif "2020-prices" in url:
                if "jun-2020" in url:
                    mes_ano = "june2020"
                elif "sept-2020" in url:
                    mes_ano = "september2020"
                elif "oct-2020" in url:
                    mes_ano = "october2020"
                elif "nov-2020" in url:
                    mes_ano = "november2020"
                elif "dec-2020" in url:
                    mes_ano = "december2020"
                else:
                    mes_ano = data_da_url.lower().replace("-", "")
            elif "2021-prices" in url:
                if "feb-2021" in url:
                    mes_ano = "february2021"
                elif "mac-2021" in url:
                    mes_ano = "march2021"
                elif "aug-2021" in url:
                    mes_ano = "august2021"
                elif "sept-2021" in url:
                    mes_ano = "september2021"
                elif "oct-2021" in url:
                    mes_ano = "october2021"
                elif "nov-2021" in url:
                    mes_ano = "november2021"
                elif "dec-2021" in url:
                    mes_ano = "december2021"
            elif "2022-prices" in url:
                if "jan-2022" in url:
                    mes_ano = "january2022"
                elif "feb-2022" in url:
                    mes_ano = "february2022"
                elif "mac-2022" in url:
                    mes_ano = "march2022"
                elif "apr-2022" in url:
                    mes_ano = "april2022"
                elif "jun-2022" in url:
                    mes_ano = "june2022"
                elif "aug-2022" in url:
                    mes_ano = "august2022"
                elif "sept-2022" in url:
                    mes_ano = "september2022"
                elif "oct-2022" in url:
                    mes_ano = "october2022"
                elif "nov-2022" in url:
                    mes_ano = "november2022"
                elif "dec-2022" in url:
                    mes_ano = "december2022"
                else:
                    mes_ano = data_da_url.lower().replace("-", "")
            elif "2023-prices" in url:
                if "mac-2023" in url:
                    mes_ano = "march2023"
                else:
                    mes_ano = "{}{}".format(data_da_url.lower().replace("-", ""), "2023")
            else:
                mes_ano = data_da_url.lower().replace("-", "")
            request = requests.get(url)
            sopa = soup(request.content, "html.parser")
            #table = sopa.findAll("div", {"class" : "dataTables_scroll"})
            table = sopa.find("div", {"class" : "droptablesresponsive dataTables-droptables droptablestable"})
            table = table.find("table")
            self.df = pd.read_html(str(table), skiprows=4)[0]
            value = "Average"
            n = self.df.stack().loc[lambda x: x == value].index[0][0]
            self.df = self.df.iloc[:n]
            self.df["Date"] = self.df["Date"].apply(lambda x: datetime.strptime(f"{x}{mes_ano}", "%d%B%Y").strftime("%d-%m-%Y") if pd.notnull(x) else np.nan)
            self.df = self.df.replace("N.A.", "0.0")
            print(self.df)
            self.collected.append(self.df)
            if "2023-prices/december" in url:
                break
        self.df_concat = pd.concat(self.collected)
    def tratamento(self):
        self.df_melted = self.df_concat.melt(id_vars='Date', var_name='code', value_name='Valor')
        self.df_excel = pd.read_excel('INTL_AseanRubber.xlsx')
        self.df_excel['code'] = self.df_excel['code'].astype(str)
        self.df_melted['code'] = self.df_melted['code'].astype(str)
        self.df_merged = self.df_melted.merge(self.df_excel, on='code')
        self.df_merged = self.df_merged.rename(columns={'Date': 'Data'})
        self.df_final = self.df_merged[['Data', 'ID', 'Valor']]
        self.df_final.to_excel("tremfinal.xlsx", index=False)

    
def main():
    bot = INTL_AseanRubber()
    bot.coleta()
    bot.tratamento()

if __name__ == "__main__":
    main()