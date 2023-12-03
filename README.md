# API Key Configuration

Before running the scripts, you need to set up your API key. Follow these steps:

1. Create a file named `.env` in the same directory as your scripts.

1. Inside the `.env` file, add your API key in the following format:

```env
API_KEY=your-api-key
```
Replace __`your-api-key`__ with your actual API key.

Ensure that you keep your API key secure and do not share it publicly. This configuration allows the scripts to access the API with the provided key.

# Dependencies

## virtualenv

1. windows

`python -m venv venv`

`.\venv\Scripts\Activate.ps1`

or
1. linux

`python3 -m venv venv`

`./venv/Scripts/activate`

2. Installing Dependencies

`pip install -r requirements.txt`


## poetry

1. `poetry install`



# Script Usage Explanation
## `main.py`

The main.py script has been enhanced to provide more flexibility in its execution.

- To collect data from the API and store it in the date folder as JSON files, use the command:
    
  - `python main.py`

    OR
  - `python3 main.py`

- To load data from the existing JSON files in the date folder into a Pandas DataFrame, use the following command:

  - `python main.py df`

__Note__: The extraction process always checks the name of the last created JSON file and begins collecting data from the date of the latest JSON. This ensures that only the most recent data is added to the files.

These changes allow you to choose between collecting new data from the API or utilizing the existing data in the date folder based on the provided argument.
# If you have any questions or need further assistance, feel free to ask! :)