import ccxt
import mysql.connector
import pandas as pd
from datetime import datetime
import schedule
import time
import threading
import keyboard

# Параметры для подключения к базе данных
db_config = {
    'host': 'localhost', #localhost:3306
    'user': 'root',
    'password': 'root',
    'database': 'tradekad'
}

# Создание подключения к базе данных
db_connection = mysql.connector.connect(**db_config)
db_cursor = db_connection.cursor()

# Создание таблицы (если она не существует)
create_table_query = '''
CREATE TABLE IF NOT EXISTS btc_data_1h (
    id INT AUTO_INCREMENT PRIMARY KEY,
    coin_name VARCHAR(10),
    timeframe VARCHAR(10),
    timestamp BIGINT,
    datetime DATETIME,
    low FLOAT,
    high FLOAT,
    open FLOAT,
    close FLOAT,
    volume FLOAT,
    max_min_ratio FLOAT,
    open_close_ratio FLOAT,
    min_max_open_close_ratio FLOAT,
    timestamp_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
'''
db_cursor.execute(create_table_query)
db_connection.commit()

'''
#часть кода для непрерывного парсинга каждый час
def parse_and_store_data(symbol, timeframe):
    # Вставьте сюда ваш код для парсинга и записи данных в базу
    

#def stop_program(): # Функция для остановки программы
#    print("Завершение программы...")
#    exit() 

# Определите расписание выполнения задачи (например, каждый час)
schedule.every().hour.do(parse_and_store_data)

# Создание потока для ожидания нажатия клавиши "q"
stop_thread = threading.Thread(target=keyboard.wait, args=('q',), daemon=True)
stop_thread.start()

# Бесконечный цикл для ожидания выполнения задачи по расписанию
while True:
    schedule.run_pending()
    if stop_thread.is_alive():
        # Если поток для ожидания нажатия клавиши "q" жив, продолжаем
        time.sleep(1)  # Пауза для предотвращения загрузки процессора
    else:
        # Если поток остановлен (нажата клавиша "q"), завершаем выполнение
        #stop_program()
    
#завершение кода непрерывного парсинга
'''

# переработай скрпит: должен брать разницу между текущим timestamp и последним в таблице + N периодов RSI, а вот записывать уже от последнего т.е. в памяти держим переменную
#с последним штампом. Запись идет до предпоследнего включительно, т.к. последний еще не отработал и данные не корректны. 


# Инициализация объекта ccxt для работы с биржей (например, Binance) 
#Certified_Cryptocurrency_Exchanges = ['binance', 'huobi', 'okx']
exchange = ccxt.binance() 

# Настройка параметров
symbol = 'BTC/USDT'
timeframe = '1m'
limit = 5000

# Получение последнего временного штампа из таблицы
last_timestamp_query = 'SELECT MAX(timestamp) FROM btc_data_1m WHERE coin_name = %s AND timeframe = %s'
last_timestamp_values = (symbol, timeframe)
db_cursor.execute(last_timestamp_query, last_timestamp_values)
last_timestamp_result = db_cursor.fetchone()
last_timestamp = last_timestamp_result[0] if last_timestamp_result[0] else 0

# Получение котировок
ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit = limit) 

# Cоздание датафрейма пандас для расчета доп. параметров тех. анализа
columns = ['timestamp', 'open_price', 'high', 'low', 'close_price', 'volume']
df_ohlcv = pd.DataFrame(ohlcv, columns=columns)

# Функция расчета RSI - на вход подается столбец close_price и желаемый период окна расчета
def calculate_rsi(data, period =14):
    delta = data.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# Запись данных в базу данных - доработать скрипт, чтобы расчет начинался со "строка last_timestamp" - 30 периодов до -1 строки
i = 0
for data in ohlcv[:-1]: #исключаем последнюю незавершенную свечу
    timestamp, open_price, high, low, close_price, volume = data
    
    # Расчет дополнительных параметров
    max_min_ratio = abs(high - low)
    open_close_ratio = abs(open_price - close_price)
    min_max_open_close_ratio = max_min_ratio / open_close_ratio # большое значение + большой объем - паттерн изменения цены
    datetime_value = datetime.fromtimestamp(timestamp / 1000)  # Преобразование временного штампа в datetime Делим на 1000 для получения секунд
    if i >= 1:
        variation_d = 1 if close_price > df_ohlcv['close_price'][i-1] else 0 # если цена закрытия выше предыдущей - 1 иначе 0 
    #расчет rsi
    if i < 5:
        rsi_5 = None
    else:
        rsi_5 = calculate_rsi(df_ohlcv[i-4:i+1]['close_price'], period=5).iloc[-1]
    if i < 14:
        rsi_14 = None
    else:
        rsi_14 = calculate_rsi(df_ohlcv[i-13:i+1]['close_price'], period=14).iloc[-1]
    if i < 21:
        rsi_21 = None
    else:
        rsi_21 = calculate_rsi(df_ohlcv[i-21:i+1]['close_price'], period=21).iloc[-1]
    if i < 30:
        rsi_30 = None
    else:
        rsi_30 = calculate_rsi(df_ohlcv[i-29:i+1]['close_price'], period=30).iloc[-1]


    # data.extend([max_min_ratio, open_close_ratio, min_max_open_close_ratio, datetime_value]) !!! использовать данную строку, если данные для прогноза будут не из бд
    i += 1
    if timestamp > last_timestamp:
        insert_query = '''
        INSERT INTO btc_data_1m (coin_name, timeframe, timestamp, datetime_value, low, high, open, close, volume, max_min_ratio, open_close_ratio, min_max_open_close_ratio)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        values = (symbol, timeframe, timestamp, datetime_value, low, high, open_price, close_price, volume, max_min_ratio, open_close_ratio, min_max_open_close_ratio) #заполни весь список
        db_cursor.execute(insert_query, values)
        db_connection.commit()

# Закрытие подключения
db_cursor.close()
db_connection.close()
