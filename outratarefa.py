import pandas as pd
from bs4 import BeautifulSoup as soup
import requests


url = "https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/consumer-price-index-australia/sep-quarter-2023"
res = requests.get(url)
sopa = soup(res.content, "html.parser")
table = sopa.find("div", {"class" : "file-description-link-right"})

div = [div for div in table if "Download" in table.text]

download_link = "{}{}".format("https://www.abs.gov.au", div[0].find("a").get("href"))
response = requests.get(download_link)
bytes_data = response.content
excel_data = pd.read_excel((bytes_data), sheet_name=None)


first_sheet_name = list(excel_data.keys())[0]
del excel_data[first_sheet_name]


df_combined = pd.concat(excel_data.values(), ignore_index=True)
df_combined = df_combined.iloc[8:]
df = df_combined
df = df.dropna(subset=[df.columns[0]])
indice = df[df.columns[0]].str.contains("Series ID").tolist()
indice = indice.index(True)
df.columns = df.iloc[indice]
df = df.iloc[indice+1:]


#df = excel_data["Data1"]
#output_file_path = "output.xlsx"
#df_combined.to_excel(output_file_path, index=False)





#bytes_data = response.content



#data = pd.read_excel(bytes_data)

#data.to_excel("output.xlsx", index=False)
a = 1

#data.to_excel("output.xlsx", index=False)

#with open("output.xlsx", "wb") as file:
#    file.write(response.content)


#excel_file_path = "output.xlsx"
#excel_data = pd.read_excel(excel_file_path, sheet_name=None)


#first_sheet_name = list(excel_data.keys())[0]
#del excel_data[first_sheet_name]


#output_file_path = "output.xlsx"
#with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
#    for sheet_name, df in excel_data.items():
#        df.to_excel(writer, sheet_name=sheet_name, index=False)
