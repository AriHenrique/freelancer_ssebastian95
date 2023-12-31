import requests
import json
import boto3
import time
import os
import awswrangler as wr
from datetime import datetime, timedelta

s3 = boto3.client('s3')
secrets_manager = boto3.client('secretsmanager')
athena_client = boto3.client('athena')
glue_client = boto3.client('glue')
codebuild_client = boto3.client('codebuild')


def delete_json_files(bucket_name: str = os.environ.get('bucket_raw')):
    objects = s3.list_objects_v2(Bucket=bucket_name)
    for obj in objects.get('Contents', []):
        if obj['Key'].endswith('.json'):
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
            print(f"Arquivo {obj['Key']} excluído.")

    print("Delete objects")


def start_codebuild():
    """
    Start an AWS CodeBuild project.
    """
    try:
        response = codebuild_client.start_build(
            projectName=os.environ.get('code_build_name')
        )
        print('start codebuild')
    except Exception as e:
        return {'error': str(e)}


def crawler_start():
    database_name = os.environ.get('data_base_name')
    table_name = os.environ.get('table_name')
    crawler_name = os.environ.get('crawler_name')
    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        print(f'A tabela {table_name} já existe. Não é necessário iniciar o Crawler.')
    except glue_client.exceptions.EntityNotFoundException:
        try:
            response = glue_client.start_crawler(Name=crawler_name)
            print(f'O Crawler {crawler_name} foi iniciado com sucesso!')
        except Exception as e:
            print(f"Erro ao iniciar o Crawler: {e}")


def secret_key():
    try:
        response = secrets_manager.get_secret_value(
            SecretId=os.environ.get('secret_key')
        )
        secret_data = json.loads(response['SecretString'])
        api_key = secret_data['api_key']
        return api_key
    except Exception as e:
        return None


def athena_query(query: str, database: str = os.environ.get('data_base_name')):
    """
    Executa uma consulta no AWS Athena.

    Args:
    - query (str): A consulta Athena a ser executada.
    - database (str): O nome do banco de dados Athena.
    - output_location (str): O local onde os resultados da consulta serão armazenados.

    Returns:
    - list: Lista de dicionários representando as linhas de resultados.
    """
    output_location: str = f"{os.environ.get('output_location')}lambda"
    try:
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=output_location,
        )
        rows = df.to_dict(orient='records')
        return rows
    except Exception as e:
        print(f"A consulta falhou com o seguinte erro: {str(e)}")
        return []


def create_table(query: str, database: str = os.environ.get('data_base_name')):
    """
    Executa uma consulta no AWS Athena.

    Args:
    - query (str): A consulta Athena a ser executada.
    - database (str): O nome do banco de dados Athena.
    - output_location (str): O local onde os resultados da consulta serão armazenados.

    Returns:
    - list: Lista de dicionários representando as linhas de resultados.
    """
    output_location: str = f"{os.environ.get('output_location')}lambda"
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': output_location
        }
    )
    query_execution_id = response['QueryExecutionId']
    while True:
        query_execution = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_execution['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)
    if status != 'SUCCEEDED':
        print(f"A consulta falhou com o status: {status}")
        return []
    results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    rows = []
    columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]

    for result in results['ResultSet']['Rows'][1:]:
        row = [field.get('VarCharValue', '') for field in result['Data']]
        rows.append(dict(zip(columns, row)))

    return rows


class Financial:
    """
    Class to make requests to the Financial Modeling Prep API.
    """
    def __init__(self,
                 start_date: datetime = datetime.strptime(create_table("""SELECT max(date) result FROM "ref_financial-data_dev".earning_calendar""")[0]['result'], '%Y-%m-%d').date(),
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
        self.start_date = start_date
        self.end_date: datetime = self.start_date + timedelta(days=range_days)
        self.range_days = range_days
        self.__symbol = symbol
        self.get_api = get_api
        self.__api_key = secret_key()

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
            # 'EPSTTM': 0.0, 'EPSTTMIDY': 0.0, 'revenue': 0.0, 'revenuePerShare': 0.0, ...
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


class UploadS3:
    """
    Class to save files in their respective folders
    """
    def __init__(self, file: list, folder_save: str = 'earning_calendar',
                 bucket: str = os.environ.get('bucket_raw')
                 ):
        self.folder_save = folder_save
        self.bucket = bucket
        self.file = file

    def save_s3(self):
        bucket_name = self.bucket
        name = (str(datetime.now())
                .replace(' ', '')
                .replace(':', '')
                .replace('.', '')
                .replace('-', ''))

        envio = str()
        new = dict(data_process=str(datetime.now().date()))
        if len(self.file) == 0:
            return
        if self.folder_save != 'historical_price_full':
            for dados in self.file:
                envio = envio + '{}\n'.format(json.dumps(dados))
        else:
            for dados in self.file:
                for e in dados:
                    if type(dados[e]) != list:
                        new[e] = dados[e]
                    else:
                        for i in dados[e]:
                            for x in i:
                                new[f'{e}_{x}'] = i[x]
                            envio = envio + '{}\n'.format(json.dumps(new))

        s3_file_path = f'{self.folder_save}/{name}.json'
        s3.put_object(Bucket=bucket_name, Key=s3_file_path, Body=envio)

