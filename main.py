import requests
import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

earnings_data = []


def get_latest_file_name(folder_path: str = 'date'):
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
    url = f"https://financialmodelingprep.com/api/v3/earning_calendar?from={_start_date}&to={_end_date}&apikey={_api_key}"

    try:
        response = requests.get(url)
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


if __name__ == "__main__":
    api_key = os.getenv("API_KEY")
    latest_file_name = get_latest_file_name()

    if latest_file_name:
        start_date = datetime.strptime(latest_file_name, '%Y-%m-%d')
    else:
        start_date = datetime.strptime('2018-01-01', '%Y-%m-%d')

    end_date = start_date + timedelta(days=2)
    current_date = datetime.now()
    count = 0

    while end_date < current_date:
        name_json = end_date.date()
        start_date += timedelta(days=2)
        end_date += timedelta(days=2)
        earnings_data.append(get_earning_calendar(api_key, start_date, end_date))
        count += 1
        if count == 15:
            dados = dict(result=earnings_data[0])
            with open(f'date/{name_json}.json', '+w') as f:
                json.dump(dados, f)
            earnings_data = []
            print(f'-----save {name_json}------')
