# Standard imports
from typing import List
import os
import pandas as pd
from costdrivers import *
import logging
import warnings

# Crawler custom imports
import requests
from bs4 import BeautifulSoup as soup
import io
from datetime import datetime

# Disable warnings globally
warnings.filterwarnings("ignore")

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


class ARG_INDEC(monthly_process_wrapper):
    """
    Crawler ARG_INDEC

    Attributes

    - logger: logging.Logger
        - An instance of the logging module to record and view events.
    - bot_instance:
        - An instance of a bot for finding data errors.
    - errors: list
        - List of found data errors
    - df_excel: pd.DataFrame
        - Dataframe with excel data (ID, indice...)
    - df_read: pd.DataFrame
        - Dataframe with collected data
    - df_results: pd.DataFrame
        - Final dataframe
    - MESES: dict
        - Dict to format date values
    """   

    def __init__(self) -> None:
        super().__init__()
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.errors: List[str] = []    

        self.df_excel = pd.DataFrame()
        self.df_colected = pd.DataFrame()
        self.df_results = pd.DataFrame(columns=['ID', 'Data', 'Valor'])  

        self.MESES = {
            'Ene' : '01',
            'Feb' : '02',
            'Mar' : '03',
            'Abr' : '04',
            'May' : '05',
            'Jun' : '06',
            'Jul' : '07',
            'Ago' : '08',
            'Sep' : '09',
            'Oct' : '10',
            'Nov' : '11',
            'Dic' : '12',
            'Enero' : '01',
            'Febrero' : '02',
            'Marzo' : '03',
            'Abril' : '04',
            'Mayo' : '05',
            'Junio' : '06',
            'Julio' : '07',
            'Agosto' : '08',
            'Septiembre' : '09',
            'Octubre' : '10',
            'Noviembre' : '11',
            'Diciembre' : '12',
            '1º trimestre' : '01-01-',
            '2º trimestre' : '01-04-',
            '3º trimestre' : '01-07-',
            '4º trimestre' : '01-10-'
            }


    def start_urls(self) -> None:
        """
        Generate a list of URLs to be scraped
        """
        pass
        self.logger.info(f"Generating data URLs")



    def read_excel(self) -> None:
        """
        Read excel file and extract ID information
        """
        if 'airflow' in os.getcwd():
            self.filename = "/usr/local/airflow/dags/ARG_INDEC.xlsx"
        else:
            self.filename = "ARG_INDEC.xlsx"

        self.df_excel = pd.read_excel(self.filename, dtype={'ID':str})
        self.df_excel["map"] = self.df_excel["map"].apply(lambda x: str(x)).str.replace("'", "")
        self.df_excel["sub"] = self.df_excel["sub"].str.replace("'", "")
        self.df_excel = self.df_excel.dropna(subset=['ID'])


    def read_data(self) -> None:
        """
        read data from URLs: 
        - Run all 10 process
        - Read and transform data
        """
        self.logger.info(f"Starting read data process")
        
        list_df_read = []
        list_of_process = [
            # self.__process1,
            # self.__process2,
            # self.__process3,
            # self.__process4,
            # self.__process5,
            # self.__process6,
            # self.__process7,
            # self.__process8,
            # self.__process9,
            self.__process10,
        ]

        for proc in list_of_process:
            df = proc(self.df_excel)

            if  df.shape[0] > 0:
                list_df_read.append(df)
            else:
                self.logger.info(f"No data found for {proc.__name__}")
                self.errors.append(f'* Dataframe {proc.__name__} está vazio')

        self.df_colected = pd.concat(list_df_read)
        
        # Raise excepetion if df_read is empty
        if self.df_colected.shape[0] == 0:
            raise Exception('Leitura finalizada com dataframe vazio!')


    def __format_df_to_upload(self) -> None:
        """
        Format dataframe to upload

        Returns:
            pd.DataFrame: dataframe to upload
        """
        
        df = self.df_results
        df['Data'] = pd.to_datetime(df['Data'], format='%d-%m-%Y')
        df = df.astype({"ID": str, "Valor": float})
        df = df[['ID', 'Data', 'Valor']]
        return df


    def transform_data(self) -> None:
        """
        Transform data:
        - Filter 's' values
        - Format dataframe to upload
        """        
        self.logger.info(f"Starting transform data process")
        
        self.df_results = self.df_colected.loc[self.df_colected['Valor'] != 's']
        self.df_results = self.__format_df_to_upload()
        

    def validate(self) -> None:
        """
        Validate data with Costdrivers platform
        """
        val = BotValidation(self.df_results)
        self.validated = val.compare_dataframes()


    def upload_info(self) -> None:
        """
        Upload data to Costdrivers platform
        """
        upload = UploadData(self.validated, self.__class__.__name__)
        upload.upload_data()
        upload.conferencia()


    def __getLinkIPC(self):
        """
        Get IPC URLs

        Returns:
            list: list of URLs
        """
        url_IPC = 'https://www.indec.gob.ar/Nivel4/Tema/3/5/31'
        html = self.__mkHtml(url_IPC)
        texto = 'Índices y variaciones porcentuales mensuales e interanuales según divisiones de la canasta, bienes y servicios, clasificación de grupos.'
        A1 = ['https://www.indec.gob.ar'+a.attrs['href'] for a in html.findAll('a') if texto in a.text][0]
        texto = 'Índices y variaciones porcentuales mensuales e interanuales según principales aperturas de la canasta.'
        A2 = ['https://www.indec.gob.ar'+a.attrs['href'] for a in html.findAll('a') if texto in a.text][0]
        print('getLinkIPC: OK..')
        return A1, A2
    

    def __getLinkSalario(self):
        """
        Get Salario URLs

        Returns:
            list: list of URLs
        """
        url = 'https://www.indec.gob.ar/Nivel4/Tema/4/31/61'
        html = self.__mkHtml(url)
        texto = 'Índice de salarios. Números índices y variaciones porcentuales respecto del período anterior y acumuladas, por sector.'
        A1 = ['https://www.indec.gob.ar'+a.attrs['href'] for a in html.findAll('a') if texto in a.text][0]
        print('getLinkSalario: OK..')
        print(A1)
        return A1


    def __mkHtml(self, url):
        """
        Make a request to URL and return response

        Args:
            url (str): Data URL
        """
        req = requests.get(url)
        html = soup(req.text, 'html.parser')
        return html
    

    def __getLinkSIPM(self):
        """
        Get SIPM URLs

        Returns:
            list: list of URLs
        """
        url = 'https://www.indec.gob.ar/Nivel4/Tema/3/5/32'
        texto = 'Índice de precios internos al por mayor, índice de precios internos básicos al por mayor e índice de precios básicos del productor.'
        html = self.__mkHtml(url)
        A1 = ['https://www.indec.gob.ar'+a.attrs['href'] for a in html.findAll('a') if texto in a.text][0]
        print('getLinkSIPM: OK..')
        return A1


    def __getLinkICC(self):
        """
        Get ICC URLs

        Returns:
            list: list of URLs
        """
        url = 'https://www.indec.gob.ar/Nivel4/Tema/3/5/33'
        texto = '(ICC e IPIB)'
        html = self.__mkHtml(url)
        A1 = ['https://www.indec.gob.ar'+a.attrs['href'].replace('../..', '') for a in html.findAll('a') if texto in a.text][0]
        print('getLinkICC: OK..')
        return A1
    

    def __read(self, url, aba, n):
        """
        Read data from URL (excel)

        Args:
            url (str): Excel file URL
            aba (str): sheet name
            n (int): Header row

        Returns:
            _type_: _description_
        """
        self.logger.info(f"Requesting: {url}")
        try:
            df = pd.read_excel(url, sheet_name=aba, header=n)
        except Exception as e:
            self.errors.append(f'* Erro {e} ao requisitar a página {url}')
            self.logger.info(f'* Erro {e} ao requisitar a página {url}')
            df = pd.DataFrame()
        return df
    

    # def __read_process_10(self, url, aba, n, cod):
    #     """
    #     Read data from URL (excel)

    #     Args:
    #         url (str): Excel file URL
    #         aba (str): sheet name
    #         n (int): Header row

    #     Returns:
    #         _type_: _description_
    #     """
    #     self.logger.info(f"Requesting: {url}")
    #     try:
    #         df = pd.read_excel(url, sheet_name=cod[aba.lower().replace(' ', '')], header=n)
    #     except Exception as e:
    #         self.errors.append(f'* Erro {e} ao requisitar a página {url}')
    #         self.logger.info(f'* Erro {e} ao requisitar a página {url}')
    #         df = pd.DataFrame()
    #     return df
    

    def __preencheColumns(self, row):
        """
        Change rows name

        Args:
            row (list): List of rows

        Returns:
            list: New rows
        """
        lst = ''
        newRow = []
        for ro in row:
            ro = str(ro)
            if not 'Unnamed:' in ro:
                lst = ro.replace('*','')
            newRow.append(lst)
        print('preencheColumns OK...')
        return newRow
    

    def __ajust(self, df, n):
        """
        - Fix dates
        - Fix date columns name

        Args:
            df (pd.DataFrame): Dataframe
            n (int): column reference

        Returns:
            pd.DataFrame: Modified dataframe
        """
        count = 0
        for ind, row in df.iterrows():
            if all(pd.isna(row)):
                count += 1
            else:
                break
        df.loc[count] = [str(x).replace('*', '') for x in df.loc[count]]
        df.loc[count] = df.loc[count].map(self.MESES)
        dates = ['01-'+str(x).zfill(2)+'-'+str(y) for y, x in zip(df.columns, df.loc[count])]
        df.columns = list(df.columns[:n]) + dates[n:]
        toDrop = [x for x in df.columns if 'nan' in x]
        df = df.drop(columns=toDrop)
        print('ajust OK...')
        return df
    
    # def __ajustEquipo(self, df):
    #     count = 0
    #     for ind, row in df.iterrows():
    #         if all(pd.isna(row)):
    #             count += 1
    #         else:
    #             break
    #     datinha = [f"Col{n}" for n in range(1, 5)]
    #     datinha.extend(df.columns[4:])
    #     df.columns = datinha
    #     df["Col1"] = df[["Col1", "Col2", "Col3"]].apply(lambda x: " ".join([str(y) for y in x]), axis=1)
    #     df = df.drop(columns=["Col2", "Col3"])
    #     df = df.dropna(subset=["Col4"])

    def __preJson(self, df, CODS, n):
        """
        - Add IDs
        - Data from columns to rows

        Args:
            CODS (dict): IDs dictionary
            df (pd.DataFrame): Dataframe
            n (int): column reference

        Returns:
            pd.DataFrame: Formatted dataframe
        """
        col = ['ID', 'Data', 'Valor']
        frames = []
        for COD in CODS.keys():
            subDf = df[df['Código'] == str(COD)].copy()
            subDf = subDf[subDf.columns[n:]].T
            subDf['ID'] = CODS[COD]
            subDf['Data'] = subDf.index
            if subDf.shape[1] > 2:
                subDf.columns = ['Valor', 'ID', 'Data']
                subDf = subDf.reset_index(drop=True)
                subDf = subDf[col]
                frames.append(subDf)
        df = pd.concat(frames)
        print('preJson OK...')
        return df
    

    def __mkReg(self, df):
        """
        Filter columns

        Args:
            df (pd.DataFrame): Dataframe

        Returns:
            pd.DataFrame: Filtered dataframe
        """
        lst = df.columns[0]
        mask = []
        for val in df[lst]:
            val = str(val)
            if 'Región' in val:
                lst = val
            mask.append(lst)
        cols = list(df.columns)
        df['reg'] = pd.Series(mask)
        df = df[['reg']+cols]
        print('mkReg OK...')
        return df
    

    def __loopReg(self, df, CODS2):
        """
        - Add ID
        - Format data

        Args:
            df (pd.DataFrame): Dataframe
            CODS2 (dict): Dictionary 

        Returns:
            df: Formated dataframe
        """
        frames = []
        for reg in CODS2.keys():
            CODS = CODS2[reg]
            subDf = df[df['reg'] == reg].copy()
            subDf = subDf.drop(columns=['reg'])
            subDf.columns = ['Código'] + list(subDf.columns)[1:]
            subDf = self.__preJson(subDf, CODS, 2)
            frames.append(subDf)
        df = pd.concat(frames)
        df = df.dropna()
        df['Data'] = df['Data'].apply(lambda x: x.strftime('%d-%m-%Y'))
        print('loopReg OK...')
        return df
    

    def __process1(self, IDS): 
        """
        Process1:
        - Read data from excel file
        - Filter and adjust data
        - Add ID

        Args:
            IDS (pd.DataFrame): df_excel dataframe

        Returns:
            pd.DataFrame: Dataframe with ID, Data and Valor
        """
        aba = '2 y 3 IPIB apertura'
        url = 'http://www.indec.gob.ar/ftp/cuadros/economia/op_icc_sipm_2016.xls'
        req = requests.get(url)
        CODS = IDS[IDS['process'] == 'process1'].copy()
        CODS = {row['map']:row['ID'] for _, row in CODS.iterrows()}
        
        with io.BytesIO(req.content) as fh:
            try:
                df = pd.io.excel.read_excel(fh, sheet_name=aba, header=8)
            except Exception as e:
                self.errors.append(f'* Erro {e} ao requisitar a página {url}')
                self.logger.info(f'* Erro {e} ao requisitar a página {url}')
                df = pd.DataFrame()
                
        if df.shape[0] > 0:
            df.columns = self.__preencheColumns(df.columns)
            df = self.__ajust(df, 4)
            df = self.__preJson(df, CODS, 4)
            print('process1 OK...')
        return df
    

    def __ICCalc(self, df):
        """
        - Calculate ICC values

        Args:
            df (pd.DataFrame): Dataframe

        Returns:
            pd.DataFrame: Modified dataframe
        """
        frames = []
        df['Valor'] = df['Valor'].apply(lambda x: float(x.replace(',','.')) if type(x) == str else x)
        df['Valor'] = df['Valor'].apply(lambda x: ((x/100)+1))
        for ID in set(df.ID):
            lst = 0
            subDf = df[df['ID'] == ID].copy()
            subDf = subDf.reset_index(drop=True)
            subFrames = []
            for i in range(subDf.shape[0]):
                if i ==0:
                    value = subDf['Valor'][i] * 100
                else:
                    value = lst * subDf['Valor'][i]
                lst = value
                subFrames.append(value)
            subDf['Valor'] = pd.Series(subFrames)
            frames.append(subDf)
        df = pd.concat(frames)
        return df
    

    def __process2(self, IDS):
        """
        Process2:
        - Read data from excel file
        - Filter and adjust data
        - Add ID

        Args:
            IDS (pd.DataFrame): df_excel dataframe

        Returns:
            pd.DataFrame: Dataframe with ID, Data and Valor
        """
        aba = 'Nivel general y capítulos'
        url = 'https://www.indec.gob.ar/ftp/cuadros/economia/icc_variaciones_2016.xls'
        CODS = IDS[IDS['process'] == 'process2'].copy()
        CODS = {row['map']:row['ID'] for _, row in CODS.iterrows()}
        df = self.__read(url, aba, 3)
        if df.shape[0] > 0:
            df = df.dropna(how='all')
            df = df.dropna(how='all', axis=1)
            df.reset_index(drop=True, inplace=True)
            df.loc[0] = df.loc[0].fillna(method='ffill')
            df.columns = ['01-' + self.MESES[mes[:3]]+ '-' + str(ano)[:-2] if 'str' in str(type(mes)) else 'drop' for mes, ano in zip(df.loc[1], df.loc[0])]
            df = df.dropna(subset=['drop', df.columns[1]])
            df.columns = ['Código'] + list(df.columns[1:])
            df = df.melt(id_vars=['Código'], var_name='Data', value_name='Valor')
            df['ID'] = df['Código'].map(CODS)
            df = self.__ICCalc(df)
            df = df[['ID', 'Data', 'Valor']]
            print('process2 OK...')
        return df
    

    def __drop_n_melt(self, df, CODS):
        """
        - Drop NA columns
        - Change columns data type
        - Map IDs
        - Melt dataframe

        Args:
            df (pd.DataFrame): Dataframe
            CODS (dict): Dictionary 

        Returns:
            pd.DataFrame: Modified dataframe
        """

        df = df.dropna(subset=[df.columns[0]])
        df = df.dropna(subset=[df.columns[1]])
        df[df.columns[0]] = df[df.columns[0]].astype(str)
        df[df.columns[0]] = df[df.columns[0]].apply(lambda x: x[1:] if x[0] == '0' else x)
        df[df.columns[0]] = df[df.columns[0]].map(CODS)
        df = df.dropna(subset=[df.columns[0]])
        df = df.melt(id_vars=[df.columns[0]], var_name='Data', value_name='Valor')
        df.columns = ['ID', 'Data', 'Valor']
        print('drop_n_melt: OK..')  

        return df
    

    def __process3(self, IDS):
        """
        Process3:
        - Read data from excel file
        - Filter and adjust data
        - Add ID

        Args:
            IDS (pd.DataFrame): df_excel dataframe

        Returns:
            pd.DataFrame: Dataframe with ID, Data and Valor
        """
        CODS2 = {
            'IPIB' : {row['map']:str(row['ID']) for _, row in IDS[(IDS['process'] == 'process3') & (IDS['sub'] == 'IPIB')].iterrows()},
            'IPIM' : {row['map']:str(row['ID']) for _, row in IDS[(IDS['process'] == 'process3') & (IDS['sub'] == 'IPIM')].iterrows()},
            'IPP' : {row['map']:str(row['ID']) for _, row in IDS[(IDS['process'] == 'process3') & (IDS['sub'] == 'IPP')].iterrows()},
        }
        url = self.__getLinkSIPM()
        frames = []
        for aba in CODS2.keys():
            CODS = CODS2[aba]
            df = self.__read(url, aba, 3)
            if df.shape[0] > 0:
                df.columns = self.__preencheColumns(df.columns)
                df = self.__ajust(df, 2)
                df = df.drop(columns=['Descripción'])
                df = self.__drop_n_melt(df, CODS)
                frames.append(df)
        if len(frames) > 0:
            df = pd.concat(frames)
            subdf = df[df['ID'] == '63201'].copy()
            subdf['ID'] = subdf.ID.map({'63201' : '58624'})
            df = pd.concat([df, subdf])
            print('process3 OK...')
        return df
    

    def __process4(self, IDS):
        """
        Process4:
        - Read data from excel file
        - Filter and adjust data
        - Add ID

        Args:
            IDS (pd.DataFrame): df_excel dataframe

        Returns:
            pd.DataFrame: Dataframe with ID, Data and Valor
        """
        CODS2 = {
            'Total nacional' : {row['map']:row['ID'] for _, row in IDS[(IDS['process'] == 'process4') & (IDS['sub'] == 'Total nacional')].iterrows()},
            'Región GBA' : {row['map']:row['ID'] for _, row in IDS[(IDS['process'] == 'process4') & (IDS['sub'] == 'Región GBA')].iterrows()},
            'Región Noreste' : {row['map']:row['ID'] for _, row in IDS[(IDS['process'] == 'process4') & (IDS['sub'] == 'Región Noreste')].iterrows()},
            }
        aba = 'Índices IPC Cobertura Nacional'
        url, _ = self.__getLinkIPC()
        df = self.__read(url, aba, 5)
        if df.shape[0] > 0:
            df = self.__mkReg(df)
            df = self.__loopReg(df, CODS2)
            print('process4 OK...')
        return df
    

    def __process5(self, IDS):
        """
        Process5:
        - Read data from excel file
        - Filter and adjust data
        - Add ID

        Args:
            IDS (pd.DataFrame): df_excel dataframe

        Returns:
            pd.DataFrame: Dataframe with ID, Data and Valor
        """
        CODS2 = {
            'Región Cuyo' : {row['map']:row['ID'] for _, row in IDS[(IDS['process'] == 'process5') & (IDS['sub'] == 'Región Cuyo')].iterrows()},
            'Región GBA' : {row['map']:row['ID'] for _, row in IDS[(IDS['process'] == 'process5') & (IDS['sub'] == 'Región GBA')].iterrows()},
            'Región Noroeste' : {row['map']:row['ID'] for _, row in IDS[(IDS['process'] == 'process5') & (IDS['sub'] == 'Región Noroeste')].iterrows()},
            'Región Pampeana' : {row['map']:row['ID'] for _, row in IDS[(IDS['process'] == 'process5') & (IDS['sub'] == 'Región Pampeana')].iterrows()},
            'Región Patagonia' : {row['map']:row['ID'] for _, row in IDS[(IDS['process'] == 'process5') & (IDS['sub'] == 'Región Patagonia')].iterrows()},
            'Región Noreste' : {row['map']:row['ID'] for _, row in IDS[(IDS['process'] == 'process5') & (IDS['sub'] == 'Región Noreste')].iterrows()},
            }
        aba = 'Índices aperturas'#'Índices IPC Cobertura Nacional'
        _, url = self.__getLinkIPC()
        df = self.__read(url, aba, 5)
        if df.shape[0] > 0:
            df = self.__mkReg(df)
            df = self.__loopReg(df, CODS2)
            print('process5 OK...')
        return df
    

    def __process6(self, IDS):
        """
        Process6:
        - Read data from excel file
        - Filter and adjust data
        - Add ID

        Args:
            IDS (pd.DataFrame): df_excel dataframe

        Returns:
            pd.DataFrame: Dataframe with ID, Data and Valor
        """
        CODS = IDS[IDS['process'] == 'process6'].copy()
        CODS = {row['map']:row['ID'] for _, row in CODS.iterrows()}
        aba = 'GBA'
        url = 'https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_precios_promedio.xls'
        df = self.__read(url, aba, 2)
        if df.shape[0] > 0:
            df.columns = self.__preencheColumns(df.columns)
            df.columns = [x.replace('Año ', '') for x in df.columns]
            df = self.__ajust(df, 2)
            df = df.drop(columns=['Unidad de medida'])
            df.columns = ['Código'] + list(df.columns)[1:]
            df = self.__preJson(df, CODS, 4)
            print('process6 OK...')
        return df
    

    def __preenche(self, col):
        """
        Change rows name

        Args:
            col (pd.Series): Pandas series

        Returns:
            list: Modified pandas series data
        """
        lst = ''
        newCol = []
        for co in col:
            if not pd.isna(co):
                co = str(co)
                lst = co.replace('*', '')
            newCol.append(lst)
        print('preenche OK...')
        return newCol
    

    def __preJson2(self, df, CODS):
        """
        - Add IDs

        Args:
            CODS (dict): IDs dictionary
            df (pd.DataFrame): Dataframe

        Returns:
            pd.DataFrame: Formatted dataframe
        """
        col = ['ID', 'Data', 'Valor']
        frames = []
        for COD in CODS.keys():
            subDf = df[['Data', COD]].copy()
            subDf.columns = ['Data', 'Valor']
            subDf['ID'] = CODS[COD]
            subDf = subDf.reset_index(drop=True)
            subDf = subDf[col]
            frames.append(subDf)
        df = pd.concat(frames)
        print('preJson2 OK...')
        return df
    

    def __process7(self, IDS):
        """
        Process7:
        - Read data from excel file
        - Filter and adjust data
        - Add ID

        Args:
            IDS (pd.DataFrame): df_excel dataframe

        Returns:
            pd.DataFrame: Dataframe with ID, Data and Valor
        """
        CODS = IDS[IDS['process'] == 'process7'].copy()
        CODS = {row['map'].replace("\\n", "\n"):row['ID'] for _, row in CODS.iterrows()}
        aba = '2'
        year = datetime.now().year
        url = f'https://www.indec.gob.ar/ftp/cuadros/economia/sh_issp_{str(year)}.xls'
        #url = 'https://www.indec.gob.ar/ftp/cuadros/economia/sh_issp_2021.xls'
        df = self.__read(url, aba, 2)
        if df.shape[0] > 0:
            df['Período'] = self.__preenche(df['Período'])
            df = df[~pd.isna(df[df.columns[1]])]
            cols = list(df.columns)
            df['Data'] = ['01-'+self.MESES[x.replace('*','')].zfill(2)+'-'+y for y, x in zip(df['Período'].astype(str), df[df.columns[1]].astype(str))]
            df = df[['Data']+cols[2:]]
            df = self.__preJson2(df, CODS)
            print('process7 OK...')
        return df
    

    def __process8(self, IDS):
        """
        Process8:
        - Read data from excel file
        - Filter and adjust data
        - Add ID

        Args:
            IDS (pd.DataFrame): df_excel dataframe

        Returns:
            pd.DataFrame: Dataframe with ID, Data and Valor
        """
        CODS = IDS[IDS['process'] == 'process8'].copy()
        CODS = {row['map']:row['ID'] for _, row in CODS.iterrows()}
        aba = 'cuadro 1'
        month = datetime.now().month
        year = datetime.now().year

        if month == 1:
            month = '12'
            year = year - 1
        elif month == 2:
            month = '11'
            year = year - 1
        elif month < 11:
            month = '0' + str(month - 2)
        else:
            month = str(month - 2)

        url = f'https://www.indec.gob.ar/ftp/cuadros/economia/sh_oferta_demanda_{month}_{str(year)}.xls'
        print(url)
        url = 'https://www.indec.gob.ar/ftp/cuadros/economia/sh_oferta_demanda_03_21.xls'
        df = self.__read(url, aba, 3)
        if df.shape[0] > 0:
            df.columns = self.__preencheColumns(df.columns)
            df.columns = [x.split(' ')[0] for x in df.columns]
            df.loc[0] = df.loc[0].map(self.MESES)
            df.columns = [str(x)+str(y) for x, y in zip(df.loc[0], df.columns)]
            df.columns = ['Código'] + list(df.columns)[1:]
            toDrop = [x for x in df.columns if 'nan' in x]
            df = df.drop(columns=toDrop)
            df = self.__preJson(df, CODS, 4)
            print('process8 OK...')
        return df
    

    def __process9(self, IDS):
        """
        Process9:
        - Read data from excel file
        - Filter and adjust data
        - Add ID

        Args:
            IDS (pd.DataFrame): df_excel dataframe

        Returns:
            pd.DataFrame: Dataframe with ID, Data and Valor
        """
        CODS = IDS[IDS['process'] == 'process9'].copy()
        CODS = {row['map']:row['ID'] for _, row in CODS.iterrows()}
        aba = 'Cuadro 1'
        url = self.__getLinkSalario()
        df = self.__read(url, aba, 3)
        if df.shape[0] > 0:
            df[df.columns[0]] = self.__preenche(df[df.columns[0]])
            df = df[~pd.isna(df[df.columns[1]])]
            cols = list(df.columns)
            df['Data'] = ['01-'+self.MESES[x.replace(' ', '')].zfill(2)+'-'+y.replace(' ', '') for y, x in zip(df[df.columns[0]].astype(str), df[df.columns[1]].astype(str))]
            df = df[['Data']+[cols[3]]+[cols[7]]]
            df.columns = ['Data', 'Total registrado (1) Número índice (Coluna H)', 'Sector privado registrado Número índice Coluna D']
            df = self.__preJson2(df, CODS)
            print('process9 OK...')
        return df


    def __process10(self, IDS, name_process = 'process10'):
        """
        Process10:
        - Read data from excel file
        - Filter and adjust data
        - Add ID

        Args:
            IDS (pd.DataFrame): df_excel dataframe

        Returns:
            pd.DataFrame: Dataframe with ID, Data and Valor
        """
        url = self.__getLinkICC()
        df_geral = pd.read_excel(url, sheet_name= None)
        df_IDS_process10 = IDS.query("process == @name_process")
        sub_unico = list(df_IDS_process10['sub'].unique())
        merge_sub = [sub for sub in sub_unico if sub.lower().strip() in [sheetname.lower().strip() for sheetname in df_geral.keys()]]
        xls = pd.ExcelFile(url)
        sheets = (xls.sheet_names)
        # cods = {}
        # for s in sheets:
        #     cods[s.lower().replace(' ', '')] = s
        
        # CODS2 = {
        #     '5, ICC' : {row['map']:row['ID'] for _, row in IDS[(IDS['process'] == 'process10') & (IDS['sub'] == '5, ICC')].iterrows()},
        #     '6, Servicios' : {row['map']:row['ID'] for _, row in IDS[(IDS['process'] == 'process10') & (IDS['sub'] == '6, Servicios')].iterrows()},
        #     '7, y 8 Cap. M. Obra' : {row['map']:row['ID'] for _, row in IDS[(IDS['process'] == 'process10') & (IDS['sub'] == '7, y 8Cap. M. Obra')].iterrows()},
        #     '9, Equipos' : {row['map']:row['ID'] for _, row in IDS[(IDS['process'] == 'process10') & (IDS['sub'] == '9, Equipos')].iterrows()},
        #     '10, Cap Gtos. Grales.' : {row['map']:row['ID'] for _, row in IDS[(IDS['process'] == 'process10') & (IDS['sub'] == '10, Cap Gtos. Grales.')].iterrows()},
        #     '11 y 12 Cap. Mat.' : {row['map']:row['ID'] for _, row in IDS[(IDS['process'] == 'process10') & (IDS['sub'] == '11 y 12Cap. Mat.')].iterrows()},
        #     }
        
        #ns = [6, 6, 8, 6, 6, 8]
        #ns = df.stack().loc[lambda x: x == value].index[0][0]
        frames = []
        for aba in merge_sub:
            CODS = {row['map']:row['ID'] for _, row in df_IDS_process10[(df_IDS_process10['process'] == name_process) & (df_IDS_process10['sub'] == aba)].iterrows()}
            print(url)
            # df = df_geral[aba]
            # header = 0
            # for ind, row in df.iterrows():
            #     if all(pd.isna(row[2 :])):
            #         header += 1
            #     else:
            #         break
            # df.columns = df.iloc[header]
            # df = df.iloc[header + 1 :, :]
            df = df_geral[aba]
            value = "Oct"
            n = df.stack().loc[lambda x: x == value].index[0][0]
            df.columns = df.iloc[n - 1]
            df = df.iloc[n:, :]
            df = df.reset_index(drop=True)
            #print('Starting for aba: ', aba, ' and n= ', header[i])
            print("Starting for aba: {} and n= {}".format(aba, df.columns[0]))

            if df.shape[0] > 0:
                df.columns = self.__preencheColumns(df.columns)
                

                # add to fix IDs
                if aba == '7, y 8 Cap. M. Obra':
                    aperturas = [' '.join([str(i) for i in x]).split('.')[0] if type(x[0]) != 'NaN' else x[0] for x in df['Aperturas'].values] 
                    df['teste'] = aperturas
                    df['teste'] = df['teste'].str.replace('nan', '').str.replace(' ', '')
                    df.iloc[:,0] = df.iloc[:,0].fillna(df['teste'])   
                    
                df = self.__ajust(df, 4)
                # if not aba == "9, Equipos":
                #     df = self.__ajust(df, 4)
                # else:
                #     df = self.__ajustEquipo(df)
                


                # add to fix IDs
                if aba == '5, ICC':
                    for c in df.columns:
                        if 'código' in c:
                            df = df.drop(c, axis=1)
                            

                    for c in df.columns:
                        if 'Insumos' in c:
                            df = df.rename(columns={c: 'código'})


                ind = [i for i, val in enumerate(df.columns) if ('código' in val.lower()) or (val.count('-') == 2)]
                df = df.iloc[:, ind]

                if list(df.columns).count('Código') > 1:
                    ID = [' '.join([str(i) for i in x]).split('.')[0] if type(x[0]) != float else x[0] for x in df['Código'].values]
                    df = df.drop(columns=['Código'])
                    cols = df.columns
                    df['ID'] = ID
                    df = df[['ID'] + list(cols)]

                    # add to fix IDs
                    if aba == '7, y 8 Cap. M. Obra':
                        df['ID'] = df['ID'].str.replace(' nan nan', '')
                

                df = self.__drop_n_melt(df, CODS)
                frames.append(df)
        if len(frames) > 0:
            df = pd.concat(frames)
            print('process10 OK...')
        return df


def main():
    bot = ARG_INDEC()
    bot.ambientacao("read_excel")
    bot.coleta("start_urls", "read_data")
    bot.tratamento("transform_data")
    bot.validacao()
    bot.upload()
    print("End Main")

if __name__ == "__main__":
    main()
