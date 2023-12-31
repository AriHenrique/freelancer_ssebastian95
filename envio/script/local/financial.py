import requests
import os
import pandas as pd
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()


class Folder:
    """
    Class to save files in their respective folders
    """
    def __init__(self, folder_save: str = 'earning_calendar', folder_path: str = 'date'):
        self.folder_path = folder_path
        self.folder_save = folder_save

    def get_latest_file_name(self):
        """
        Create folder structure based on the API endpoint name.
        By default, it creates the structure date/earning_calendar.

        Returns:
        str: The latest file in alphanumeric order.
        """
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

    def save_json(self, earnings_data: list, name):
        """
        Save data to a JSON file.

        Parameters:
        earnings_data (list): List of data.
        name (str): File name.

        Returns:
        None
        """
        data = dict(result=earnings_data)
        json_file_path = f'{self.folder_path}/{self.folder_save}/{str(name)}.json'
        with open(json_file_path, '+w', encoding='utf-8') as json_file:
            json.dump(data, json_file, ensure_ascii=False)


class Financial:
    """
    Class to make requests to the Financial Modeling Prep API.
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
        """
        Initializes the Financial class.

        Parameters:
        api_key (str): Financial Modeling Prep API key.
        start_date (datetime): Initial date for the API request.
        symbol (str): Stock symbol.
        get_api (str): API endpoint.
        range_days (int): Interval days for the API request.

        Returns:
        None
        """
        self.__api_key = api_key
        self.start_date = start_date
        self.end_date: datetime = start_date + timedelta(days=range_days)
        self.range_days = range_days
        self.__symbol = symbol
        self.get_api = get_api

    def _url_list(self, url: str):
        """
        Returns the URL and parameters configuration for each API endpoint.

        Parameters:
        url (str): API endpoint name.

        Returns:
        dict: URL and parameters configuration.

        Example:
        _url_list('earning_calendar')
        # {'url': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
        #  'params': {'from': '2020-01-01', 'to': '2020-01-03', 'apikey': 'your_api_key'}}
        """
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
        """
        Make the API request and return the result in JSON format.

        Returns:
        dict: Result of the API request in JSON format.

        Example:
        response_api()
        # {'result': [{'symbol': 'AAPL', 'date': '2020-01-01', 'actualEPS': 0.0, 'consensusEPS': 0.0,
        # 'estimatedEPS': 0.0, 'numberOfEstimates': 0, 'EPSSurpriseDollar': 0.0, 'EPSReportDate': None,
        # 'fiscalPeriod': None, 'fiscalEndDate': None, 'yearAgoEPS': None, 'numberOfAnalysts': 0,
        # 'EPSTTM': 0.0, 'EPSTTMIDY': 0.0, 'revenue': 0.0, 'revenuePerShare': 0.0, 'quarterlyEarningsGrowthYOY': None,
        """
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
    """
    Class to process data returned by the Financial Modeling Prep API.
    """
    def __init__(self, folder_save: str = 'earning_calendar', folder_path: str = 'date', output_csv: str = 'data.csv'):
        """
        Initializes the Process class.

        Parameters:
        folder_path (str): Root folder name (default is 'date').
        folder_save (str): Folder name where json files are saved (default is 'earning_calendar').
        output_csv (str): CSV file name to be created (default is 'data.csv').

        Returns:
        None
        """
        self.folder_path = folder_path
        self.folder_save = folder_save
        self.__output_csv = output_csv

    def process_json_files(self, column: list = None, save: bool = False):
        """
        Process the json files saved in the 'folder_save' folder.
        Returns a dataframe with processed data.

        Parameters:
        column (list): List of columns to be extracted from the dataframe.

        Returns:
        pandas.DataFrame: Dataframe with processed data.

        Example:
        process_json_files()
        process_json_files(column=['symbol', 'date'])
        process_json_files(column=['symbol', 'date'])
        """
        dfs = []
        for file in os.listdir(f'{self.folder_path}/{self.folder_save}'):
            if file.endswith('.json'):
                file_path = os.path.join(f'{self.folder_path}/{self.folder_save}', file)
                df = pd.read_json(file_path, orient='records', encoding='utf-8')
                if 'result' in df.columns:

                    df_normalized = pd.json_normalize(df['result'])
                    df = pd.concat([df.drop('result', axis=1), df_normalized], axis=1)
                dfs.append(df)
        df_final = pd.concat(dfs, ignore_index=True).drop_duplicates()
        if column:
            df_final = df_final[column].drop_duplicates()
            df_final = df_final.sort_values(by=column[0])
            if save:
                df_final.to_csv(f'{'_'.join(column)}.csv', index=False)
            print('process df finish')
            return df_final
        if save:
            df_final.to_csv(self.__output_csv, index=False)
        print('process df finish')
        return df_final

    def remove_names(self, df: pd.DataFrame, columns: list, order_by: int = 0, asc: bool = True):
        """
        Remove duplicate data from a dataframe.
        Returns a dataframe with removed data.

        Parameters:
        df (pandas.DataFrame): DataFrame to be processed.
        columns (list): List of columns to be used to identify duplicate data.
        order_by (int): Index of the column to be used to sort the data.
        asc (bool): Indicates whether the data should be sorted in ascending or descending order.

        Returns:
        pandas.DataFrame: DataFrame with removed data.

        Example:
        remove_names(df, ['symbol', 'date'], 1, True)
        remove_names(df, ['symbol', 'date'], 1, False)
        remove_names(df, ['symbol', 'date'], 0, False)
        remove_names(df, ['symbol', 'date'], 0, True)
        """
        if not os.listdir(f'{self.folder_path}/{self.folder_save}'):
            if 'symbol' in df.columns:
                df['symbol'] = df['symbol'].apply(lambda x: x.replace('{', '').replace('}', '') if isinstance(x, str) else x)
            return df

        symbols_to_remove = set()
        for file_name in os.listdir(f'{self.folder_path}/{self.folder_save}'):
            if file_name.endswith('.json'):
                file_path = os.path.join(f'{self.folder_path}/{self.folder_save}', file_name)
                with open(file_path, 'r', encoding='utf-8') as json_file:
                    json_data = json.load(json_file)
                    if 'result' in json_data:
                        for item in json_data['result']:
                            column_values = tuple(item.get(column, None) for column in columns)
                            if None not in column_values:
                                symbols_to_remove.add(column_values)
        temp_df = pd.DataFrame(list(symbols_to_remove), columns=columns)
        df = pd.concat([df, temp_df]).drop_duplicates(subset=columns, keep=False)
        if 'symbol' in df.columns:
            df['symbol'] = df['symbol'].apply(lambda x: x.replace('{', '').replace('}', '') if isinstance(x, str) else x)
            df = df.sort_values(by=columns[order_by], ascending=asc)
        return df