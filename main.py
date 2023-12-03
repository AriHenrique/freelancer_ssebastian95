import requests
import os
import json
import pandas as pd
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

earnings_data = []
FOLDER = 'date'
OUTPUT_CSV = 'data.csv'


def process_json_files(folder_path: str = FOLDER, output_csv: str = OUTPUT_CSV):
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
    for file in os.listdir(folder_path):
        if file.endswith('.json'):
            file_path = os.path.join(folder_path, file)
            df = pd.read_json(file_path, orient='records')
            if 'result' in df.columns:
                df_normalized = pd.json_normalize(df['result'])
                df = pd.concat([df.drop('result', axis=1), df_normalized], axis=1)
            dfs.append(df)
    df_final = pd.concat(dfs, ignore_index=True).drop_duplicates()
    df_final.to_csv(output_csv, index=False)
    print('process df finish')
    return df_final


def get_latest_file_name(folder_path: str = FOLDER):
    """
    Returns the name of the most recent file in a folder.
    If the 'date' folder doesn't exist, it will be created.

    Parameters:
    - folder_path (str): The path to the folder containing the JSON files.

    Returns:
    - str: The name of the most recent file, or None if there are no files.
    """

    # Create the 'date' folder if it doesn't exist in the specified folder
    date_folder_path = os.path.join(folder_path)
    if not os.path.exists(date_folder_path):
        os.makedirs(date_folder_path)
        print(f"Folder '{folder_path}' created successfully at: {date_folder_path}")

    file_folder = os.listdir(date_folder_path)
    file_json = [file for file in file_folder if file.endswith('.json')]

    if file_json:
        file_json.sort(reverse=True)
        current_file = file_json[0].split('.')[0]
        return current_file
    else:
        print("No JSON files found in the folder.")
        return None


def get_earning_calendar(_api_key: str, _start_date: datetime, _end_date: datetime):
    print(f"start_date_loop: {_start_date}")
    print(f"end_date_loop: {_end_date}")
    url = f"https://financialmodelingprep.com/api/v3/earning_calendar?from={str(_start_date.date())}&to={str(_end_date.date())}&apikey={_api_key}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.HTTPError as errh:
        print("HTTP Error:", errh)
        return None
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
        return None
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
        return None
    except requests.exceptions.RequestException as err:
        print("Oops! Something went wrong:", err)
        return None




if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'df':
        df = process_json_files()
        print(df)
    else:
        api_key = os.getenv("API_KEY")

        latest_file_name = get_latest_file_name()

        if latest_file_name:
            start_date = datetime.strptime(latest_file_name, '%Y-%m-%d')
        else:
            start_date = datetime.strptime('2018-01-01', '%Y-%m-%d')

        end_date = start_date + timedelta(days=2)
        current_date = datetime.now()
        count = 0

        name_json = end_date.date()
        while end_date < current_date:
            earnings_data.append(get_earning_calendar(api_key, start_date, end_date))
            start_date += timedelta(days=2)
            end_date += timedelta(days=2)
            count += 1
            if count == 15:
                dados = dict(result=earnings_data[0])
                with open(f'{FOLDER}/{name_json}.json', '+w') as f:
                    json.dump(dados, f)
                earnings_data = []
                current_date = datetime.now()
                count = 0
                print(f'-----save {name_json}------')
            elif not end_date < current_date:
                dados = dict(result=earnings_data[0])
                with open(f'{FOLDER}/{current_date.date()}.json', '+w') as f:
                    json.dump(dados, f)
                print(f'-----save {name_json} finish------')

        process_json_files()
        print('updated data')
