import requests
import os
import pandas as pd
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()


class Folder:
    def __init__(self, folder_save: str = 'earning_calendar', folder_path: str = 'date'):
        self.folder_path = folder_path
        self.folder_save = folder_save

    def get_latest_file_name(self):
        """
        Returns the name of the most recent file in a folder.
        If the 'date' folder doesn't exist, it will be created.

        Parameters:
        - folder_path (str): The path to the folder containing the JSON files.

        Returns:
        - str: The name of the most recent file, or None if there are no files.
        """

        # Create the 'date' folder if it doesn't exist in the specified folder
        date_folder_path = os.path.join(self.folder_path, self.folder_save)
        if not os.path.exists(date_folder_path):
            os.makedirs(date_folder_path)
            print(f"Folder '{self.folder_save}' created successfully at: {date_folder_path}")

        file_folder = os.listdir(date_folder_path)
        file_json = [file for file in file_folder if file.endswith('.json')]

        if file_json:
            file_json.sort(reverse=True)
            current_file = file_json[0].split('.')[0]
            return current_file
        else:
            return None

    def save_json(self, earnings_data: list, end_date):
        dados = dict(result=earnings_data)
        json_file_path = f'{self.folder_path}/{self.folder_save}/{str(end_date)}.json'
        with open(json_file_path, '+w', encoding='utf-8') as json_file:
            json.dump(dados, json_file, ensure_ascii=False)


class Financial:
    """
    Classe para fazer a requisição da API do financialmodelingprep

    """
    __api_key = os.getenv("API_KEY")
    __latest_file_name = Folder().get_latest_file_name()

    if __latest_file_name:
        __start_date = (datetime.strptime(__latest_file_name, '%Y-%m-%d')).date()
    else:
        __start_date = (datetime.strptime('2018-01-01', '%Y-%m-%d')).date()

    def __init__(self, api_key: str = __api_key,
                 start_date: datetime = __start_date,
                 symbol: str = None,
                 get_api: str = 'earning_calendar', range_days: int = 2):
        self.__api_key = api_key
        self.start_date = start_date
        self.end_date = start_date + timedelta(days=range_days)
        self.range_days = range_days
        self.__symbol = symbol
        self.get_api = get_api

    def _url_list(self, url: str):
        base = 'https://financialmodelingprep.com/api/v3/'
        urls = dict(
            earning_calendar=dict(url=f"{base}earning_calendar",
                                  params={
                                      'from': str(self.start_date),
                                      'to': str(self.end_date),
                                      'apikey': self.__api_key
                                  }),

            historical_price_full=dict(url=f'{base}historical-price-full/{self.__symbol}',
                                       params={
                                           'from': str(self.start_date),
                                           'to': str(self.end_date),
                                           'apikey': self.__api_key
                                       }),

            profile=dict(url=f'{base}profile/{self.__symbol}',
                         params={
                             'apikey': self.__api_key
                         })
        )

        for i in urls:
            Folder(i).get_latest_file_name()
        if url in urls:
            return urls[url]
        else:
            return None

    def response_api(self):

        url = self._url_list(self.get_api)
        if not url:
            print('get_api name not found')
            return None

        try:
            response = requests.get(url['url'], params=url['params'])
            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as errh:
            print("HTTP Error:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            print("Timeout Error:", errt)
        except requests.exceptions.RequestException as err:
            print("Oops! Something went wrong:", err)
        return None


class Process:
    def __init__(self, folder_save: str = 'earning_calendar', folder_path: str = 'date', output_csv: str = 'data.csv'):
        self.folder_path = folder_path
        self.folder_save = folder_save
        self.__output_csv = output_csv

    def process_json_files(self, column: str = None):
        """
        Process JSON files in a specified folder, normalizing and concatenating them into a Pandas DataFrame.

        Parameters:
        - folder_path (str): Path of the directory containing JSON files (default is 'date').
        - output_csv (str): Name of the output CSV file (default is 'data.csv').

        Returns:
        - pd.DataFrame: The final DataFrame containing unique rows from processed JSON files.
        """
        # List to store individual DataFrames
        dfs = []
        for file in os.listdir(f'{self.folder_path}/{self.folder_save}'):
            if file.endswith('.json'):
                file_path = os.path.join(f'{self.folder_path}/{self.folder_save}', file)
                df = pd.read_json(file_path, orient='records')
                if 'result' in df.columns:
                    df_normalized = pd.json_normalize(df['result'])
                    df = pd.concat([df.drop('result', axis=1), df_normalized], axis=1)
                dfs.append(df)
        df_final = pd.concat(dfs, ignore_index=True).drop_duplicates()
        if column:
            df_final = df_final[[column]].drop_duplicates()
            df_final.to_csv(f'{column}.csv', index=False)
            print('process df finish')
            return df_final

        df_final.to_csv(self.__output_csv, index=False)
        print('process df finish')
        return df_final
