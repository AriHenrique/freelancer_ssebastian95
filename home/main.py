import time
import threading
import financial
from datetime import datetime, timedelta

EARNINGS_DATA = list()
THREADS = list()


def request_and_save(start_time: time.time(), fn: financial, symbol: str = None, date=None):
    """
    Function created to make parallelized API requests
    :param start_time: The start time of the request.
    :param fn: The financial object to use for the request.
    :param symbol: The symbol to use for the request.
    :param date: The date to use for the request.
    :return: True if the request was successful, False otherwise.
    """
    global THREADS, EARNINGS_DATA
    if fn.get_api == 'earning_calendar':
        tmp = fn.response_api()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f'API request {fn.end_date} completed in {elapsed_time:.2f} seconds.')
        if not tmp:
            print('API error')
            return False
        for index in tmp:
            EARNINGS_DATA.append(index)
        fn.start_date += timedelta(days=fn.range_days)
        fn.end_date += timedelta(days=fn.range_days)
        return True
    elif fn.get_api == 'profile':
        tmp = fn.response_api()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f'API request {symbol} completed in {elapsed_time:.2f} seconds.')
        if len(tmp) > 0:
            EARNINGS_DATA.append(tmp[0])
            return True
        else:
            return False
    elif fn.get_api == 'historical_price_full':
        fn.start_date = datetime.strptime(date, '%Y-%m-%d').date() - timedelta(days=10)
        fn.end_date = datetime.strptime(date, '%Y-%m-%d').date() + timedelta(days=30)
        tmp = fn.response_api()

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f'API request for {symbol} - {date} completed in {elapsed_time:.2f} seconds.')
        if len(tmp):
            EARNINGS_DATA.append(tmp)
            return True
        else:
            return False


def etl():
    """
    Initiates the earnings calendar extraction process from the API, always between two days.
    """
    global THREADS, EARNINGS_DATA
    fn = financial.Financial()
    current_date = datetime.now()
    count = 0
    start_time_local = time.time()

    while fn.end_date <= current_date.date():
        thread = threading.Thread(target=request_and_save, args=(time.time(), fn))
        THREADS.append(thread)
        thread.start()
        count += 1
        if len(THREADS) >= 5 and start_time_local - time.time() < 1:
            print('Delay...')
            for t in THREADS:
                t.join()
            THREADS = list()
            start_time_local = time.time()
        if count == 15:
            financial.UploadS3(file=EARNINGS_DATA, folder_save=fn.get_api).save_s3()
            current_date = datetime.now()
            EARNINGS_DATA = list()
            count = 0
    for t in THREADS:
        t.join()
    if len(EARNINGS_DATA) > 0:
        financial.UploadS3(file=EARNINGS_DATA, folder_save=fn.get_api).save_s3()
        EARNINGS_DATA = list()
    print(f'-----save etl {current_date.date()} finish------')


def profile():
    """
    Initiates the company profile extraction process from the API, using the company 'symbol' for search.
    """
    global THREADS, EARNINGS_DATA
    create = """
        CREATE EXTERNAL TABLE IF NOT EXISTS process_profile(
          symbol string)
        LOCATION 's3://financial-data-dev-financial-s3-bucket-raw/process_profile/'
    """
    financial.create_table(create)
    query = """
    select distinct a.symbol 
    from "raw_financial-data_dev".earning_calendar a 
    left join "ref_financial-data_dev".profile b on a.symbol = b.symbol
    left join "raw_financial-data_dev".process_profile c on a.symbol = c.symbol
    where b.symbol is null
    and c.symbol is null
    order by a.symbol
    """

    start_time_local = time.time()
    symb = None
    result = financial.athena_query(query)
    add = list()
    if len(result) == 0:
        print('Finish')
        return None
    fn = financial.Financial(get_api='profile')
    for i in result:
        for symb in i.values():
            add.append(symb)
            fn.symbol = symb
            thread = threading.Thread(target=request_and_save, args=(time.time(), fn, symb))
            THREADS.append(thread)
            thread.start()

            if len(THREADS) >= 5 and start_time_local - time.time() < 1:
                print('Delay...')
                for t in THREADS:
                    t.join()
                THREADS = list()
                start_time_local = time.time()
    if len(EARNINGS_DATA) > 0:
        financial.UploadS3(file=EARNINGS_DATA, folder_save=fn.get_api).save_s3()
        print(f'---profile {symb} finish---')
        EARNINGS_DATA = list()

    consulta_insert_multipla = """INSERT INTO process_profile (symbol) VALUES """
    consulta_insert_multipla += ", ".join([f"('{valor}')" for valor in add])

    financial.create_table(consulta_insert_multipla)
    print(f'---profile finish---')


def historical_price_full(asc: bool = True):
    """
    Initiates the extraction process of the full list of historical dividend payments for publicly traded companies,
    searching within the window of -10 to +30 days from the current date.
    """
    global THREADS, EARNINGS_DATA
    start_time_local = time.time()
    create = """
        CREATE EXTERNAL TABLE IF NOT EXISTS process_historical_price_full(
          symbol string,
          date string)
        LOCATION 's3://financial-data-dev-financial-s3-bucket-raw/process_historical_price_full/'
    """
    financial.create_table(create)
    query = """
    select distinct 
    a.symbol, 
    a.date
    from "raw_financial-data_dev".earning_calendar a 
    left join "ref_financial-data_dev".historical_price_full b 
    on a.symbol = b.symbol and a.date = b.search_date
    left join "raw_financial-data_dev".process_historical_price_full c 
    on a.symbol = c.symbol and a.date = c.date
    where b.search_date is null
    and b.symbol is null
    and c.date is null
    and c.symbol is null
    order by a.symbol
    """
    result = financial.athena_query(query)
    if len(result) == 0:
        print('Finish')
        return None
    fn = financial.Financial(get_api='historical_price_full')
    add = list()
    push = 0
    for i in result:
        add.append([i['symbol'], i['date']])
        fn.symbol = i['symbol']
        thread = threading.Thread(target=request_and_save, args=(time.time(), fn, i['symbol'], i['date']))
        THREADS.append(thread)
        thread.start()
        push+=1
        if len(THREADS) >= 5 and start_time_local - time.time() < 1:
            print(f'Delay... len list {len(EARNINGS_DATA)}')
            for t in THREADS:
                t.join()
            THREADS = list()
            start_time_local = time.time()
        if push > 500 and len(EARNINGS_DATA) == 0:

            consulta_insert_multipla = """INSERT INTO process_historical_price_full (symbol, date) VALUES """
            consulta_insert_multipla += ", ".join([f"('{valor[0]}', '{valor[1]}')" for valor in add])
            financial.create_table(consulta_insert_multipla)
            push = 0
            add = list()
            print('insert results...')

    for t in THREADS:
        t.join()
    print(EARNINGS_DATA)
    if len(EARNINGS_DATA) > 0:
        financial.UploadS3(file=EARNINGS_DATA, folder_save=fn.get_api).save_s3()
        EARNINGS_DATA = list()
    consulta_insert_multipla = """INSERT INTO process_historical_price_full (symbol, date) VALUES """
    consulta_insert_multipla += ", ".join([f"('{valor[0]}', '{valor[1]}')" for valor in add])

    financial.create_table(consulta_insert_multipla)
    print(f'---historical_price_full finish---')

if __name__ == '__main__':
    financial.delete_json_files()
    etl()
    profile()
    historical_price_full()
    # financial.crawler_start()
    financial.start_codebuild()
