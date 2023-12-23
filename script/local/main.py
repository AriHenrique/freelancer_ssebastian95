import sys
import time
import threading
import financial
from datetime import datetime, timedelta

FOLDER = 'date'
OUTPUT_CSV = 'data.csv'
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
        for i in tmp:
            EARNINGS_DATA.append(i)
        fn.start_date += timedelta(days=fn.range_days)
        fn.end_date += timedelta(days=fn.range_days)
        return True
    elif fn.get_api == 'profile':
        tmp = fn.response_api()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f'API request {symbol} completed in {elapsed_time:.2f} seconds.')
        if len(fn):
            EARNINGS_DATA.append(tmp[0])
            return True
        else:
            EARNINGS_DATA.append(dict(symbol=symbol))
            return False
    elif fn.get_api == 'historical_price_full':
        fn.start_date = datetime.strptime(date, '%Y-%m-%d').date() - timedelta(days=10)
        fn.end_date = datetime.strptime(date, '%Y-%m-%d').date() + timedelta(days=30)
        tmp = fn.response_api()

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f'API request for {symbol} completed in {elapsed_time:.2f} seconds.')

        if len(tmp):
            tmp['date'] = date
            EARNINGS_DATA.append(tmp)
            return True
        else:
            EARNINGS_DATA.append(dict(symbol=symbol, date=date))
            return False


def etl():
    """
    Initiates the earnings calendar extraction process from the API, always between two days.
    """
    global THREADS, EARNINGS_DATA
    fn = financial.Financial()
    file = financial.Folder()
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
            file.save_json(EARNINGS_DATA, fn.start_date)
            current_date = datetime.now()
            EARNINGS_DATA = list()
            count = 0
            print(f'-----save {fn.start_date}------')
    for t in THREADS:
        t.join()
    file.save_json(EARNINGS_DATA, current_date.date())
    print(f'-----save etl {current_date.date()} finish------')


def profile():
    """
    Initiates the company profile extraction process from the API, using the company 'symbol' for search.
    """
    global THREADS, EARNINGS_DATA
    file = financial.Folder('profile')
    symbol = financial.Process().process_json_files(['symbol'])
    df = financial.Process('profile').remove_names(symbol, ['symbol'])
    start_time_local = time.time()
    for index, row in df.iterrows():
        fn = financial.Financial(symbol=row['symbol'], get_api='profile')
        thread = threading.Thread(target=request_and_save, args=(time.time(), fn, row['symbol']))
        THREADS.append(thread)
        thread.start()

        if len(THREADS) >= 5 and start_time_local - time.time() < 1:
            print('Delay...')
            for t in THREADS:
                t.join()
            THREADS = list()
            start_time_local = time.time()

        if len(EARNINGS_DATA) >= 4500:
            file.save_json(EARNINGS_DATA, row['symbol'])
            EARNINGS_DATA = list()
            print(f'---save {row["symbol"]}---')

    if len(EARNINGS_DATA) != 4500:
        file.save_json(EARNINGS_DATA, row['symbol'])
        print(f'---profile {row["symbol"]} finish---')


def historical_price_full(asc: bool = True):
    """
    Initiates the extraction process of the full list of historical dividend payments for publicly traded companies,
    searching within the window of -10 to +30 days from the current date.
    """
    global THREADS, EARNINGS_DATA
    file = financial.Folder('historical_price_full')
    pro = financial.Process()
    symbol = pro.process_json_files(['symbol', 'date'])
    df = financial.Process('historical_price_full').remove_names(symbol, ['date', 'symbol'], 1, asc)
    start_time_local = time.time()
    for index, row in df.iterrows():
        fn = financial.Financial(symbol=row['symbol'], get_api='historical_price_full')
        thread = threading.Thread(target=request_and_save, args=(time.time(), fn, row['symbol'], row['date']))
        THREADS.append(thread)
        thread.start()

        if len(THREADS) >= 5 and start_time_local - time.time() < 1:
            print('Delay...')
            for t in THREADS:
                t.join()
            THREADS = list()
            start_time_local = time.time()
        if len(EARNINGS_DATA) >= 4500:
            file.save_json(EARNINGS_DATA, row['symbol'])
            EARNINGS_DATA = list()
            print(f'---save historical_price_full {row["symbol"]}---')
    for t in THREADS:
        t.join()

    print(f'---save historical_price_full {row["symbol"]} finish---')


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == 'df':
        df = financial.Process().process_json_files()
        print(df)
    elif len(sys.argv) >= 3 and sys.argv[1] == 'df':
        arg = list()
        for i in sys.argv[2::]:
            arg.append(i)
        df = financial.Process().process_json_files(arg)
    else:
        etl()
        profile()
        historical_price_full()
        print('updated data')
