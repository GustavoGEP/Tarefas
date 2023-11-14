import pandas as pd
from bs4 import BeautifulSoup as soup
import requests
from requests import Session
import openpyxl

class urubu_do_pix():
    def __init__(self):
        self.endpoint = "https://www.abs.gov.au"
        self.url = "{}/statistics/economy/price-indexes-and-inflation/consumer-price-index-australia".format(self.endpoint)
        self.request = requests.get(self.url)
        self.sopa = soup(self.request.content, "html.parser")
        self.Session = Session()
        
    def ambientacao(self):

        divs = self.sopa.findAll("a", href = True)


        filtered_divs = [div for div in divs if "Consumer Price Index" in div.get_text()][0]

        url = "{}{}".format(self.endpoint, filtered_divs.get("href"))
        request = requests.get(url)
        sopa = soup(request.content, "html.parser")
        self.table = sopa.find("div", {"class" : "file-description-link-right"})

    def coleta(self):
        div_2 = [div for div in self.table if "Download" in self.table.text]

        download_link = "{}{}".format(self.endpoint, div_2[0].find("a").get("href"))
        response = requests.get(download_link)
        bytes_data = response.content
        self.excel_data = pd.read_excel((bytes_data), sheet_name=None)
        
    def tratamento(self):

        first_sheet_name = list(self.excel_data.keys())[0]
        del self.excel_data[first_sheet_name]


        df_combined = pd.concat(self.excel_data.values(), ignore_index=True)
        df_combined = df_combined.iloc[8:]
        df = df_combined
        df = df.dropna(subset=[df.columns[0]])
        indice = df[df.columns[0]].str.contains("Series ID").tolist()
        indice = indice.index(True)
        df.columns = df.iloc[indice]
        df = df.iloc[indice+1:]
        df = pd.melt(df, id_vars=df.columns[0], var_name='Data', value_name='Valor')

        df.columns = ["Data", "ID", "Valor"]
        df["Valor"] = df["Valor"].astype(float)
        self.df = df

    def merge(self):
        aus_abs = "AUS_ABS.xlsx"
        self.aus_abs_data = pd.read_excel(aus_abs, skiprows=1)
        self.aus_abs_data.columns = [x.strip() for x in self.aus_abs_data.columns]
        self.df["ID2"] = self.df["ID"].apply(lambda x: self.verificarID(x))
        self.df = self.df.dropna(subset=["ID2"])
        self.df = self.df.drop(columns=["ID"])
        self.df = self.df.rename(columns={"ID2": "ID"})
        self.df = self.df[["ID", "Data", "Valor"]]
        self.df["ID"] = self.df["ID"].astype(int)
        self.df["ID"] = self.df["ID"].astype(str)

    def verificarID(self, row):
        trem = self.aus_abs_data.query("CÓDIGO == @row").values
        if len(trem) > 0:
            return trem[0][0]
        return None

    def upload(self):
        
        self.df.to_excel("planilhafinal.xlsx", index=False)

def main():
    bot = urubu_do_pix()
    bot.ambientacao()
    bot.coleta()
    bot.tratamento()
    bot.merge()
    bot.upload()

if __name__ == "__main__":
    main()

a = 1


