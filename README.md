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

The main.py script collects data from the API and stores it in the date folder in JSON files. Each file is named with the date of the latest data collected.

To run the script, use the following command:

`python main.py` OR `python3 main.py`

__Note__: The extraction process always checks the name of the last created JSON file and begins collecting data from the date of the latest JSON. This ensures that only the most recent data is added to the files.

## `read.ipynb`

The read.ipynb Jupyter Notebook creates a Pandas DataFrame by collecting all JSON files in the date folder into a single DataFrame. This allows for easy analysis and manipulation of the collected data.

To run the Jupyter Notebook, use the following command:

`jupyter notebook read.ipynb`

This will open the notebook in your default web browser. Execute the cells in the notebook to read and process the data.

# If you have any questions or need further assistance, feel free to ask! :)