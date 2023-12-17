import sys
import time
import financial
from datetime import datetime, timedelta


FOLDER = 'date'
OUTPUT_CSV = 'data.csv'


def etl():
    fn = financial.Financial()
    file = financial.Folder()
    earnings_data = []
    current_date = datetime.now()
    count = 0
    while fn.end_date <= current_date.date():
        tmp = fn.response_api()
        if not tmp:
            print('error API')
            return False
        for i in tmp:
            earnings_data.append(i)
        print(f'-----{fn.end_date}------')
        fn.start_date += timedelta(days=fn.range_days)
        fn.end_date += timedelta(days=fn.range_days)
        count += 1
        time.sleep(0.21)

        if count == 15:
            file.save_json(earnings_data, fn.start_date)
            earnings_data = []
            current_date = datetime.now()
            count = 0
            print(f'-----save {fn.start_date}------')

    file.save_json(earnings_data, current_date.date())
    print(f'-----save {current_date.date()} finish------')
    return True


def profile():
    df = financial.Process().process_json_files('symbol')
    file = financial.Folder('profile')
    earnings_data = []
    for index, row in df.iterrows():
        print(row['symbol'])
        fn = financial.Financial(symbol=row['symbol'], get_api='profile').response_api()
        file.save_json(fn, row['symbol'])
        time.sleep(0.21)
        break


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == 'df':
        df = financial.Process().process_json_files()
        print(df)
    elif len(sys.argv) == 3 and sys.argv[1] == 'df':
        df = financial.Process().process_json_files(sys.argv[2])

    else:
        # etl()
        profile()
        print('updated data')
