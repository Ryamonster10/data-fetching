from datetime import datetime, timedelta, time as dt_time
import time
import blpapi
import pandas as pd
import ta  # Import the ta library for technical analysis
import mysql.connector
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Bloomberg DAPI API connection details
API_HOST = 'localhost'
API_PORT = 8194

# List of NDX Index tickers
NDX_TICKERS = [
    'AAPL US Equity', 'MSFT US Equity', 'GOOGL US Equity', 'AMZN US Equity', 'NVDA US Equity'
    # Add all other tickers in the NDX Index here
]

# MySQL database connection details
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Function to check Bloomberg API availability
def check_bloomberg_api():
    """Make sure the blp api is live
    """
    session = blpapi.Session()
    if not session.start():
        print("Failed to start Bloomberg API session.")
        return False
    if not session.openService("//blp/refdata"):
        print("Failed to open Bloomberg API service.")
        return False
    return True

# Function to fetch stock data from Bloomberg DAPI API
def fetch_stock_data_from_bloomberg():
    """function to get the stock data from DAPI
    """
    session = blpapi.Session()
    session.start()
    session.openService("//blp/refdata")
    service = session.getService("//blp/refdata")

    request = service.createRequest("ReferenceDataRequest")
    for ticker in NDX_TICKERS:
        request.append("securities", ticker)

    # Specify the fields we want: Last Price and Volume
    request.append("fields", "PX_LAST")
    request.append("fields", "VOLUME")

    # Send request and wait for response
    session.sendRequest(request)

    data = []
    while True:
        ev = session.nextEvent(500)
        for msg in ev:
            if ev.eventType() in [blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE]:
                securityDataArray = msg.getElement("securityData")
                for securityData in securityDataArray.values():
                    ticker = securityData.getElementAsString("security")
                    fieldData = securityData.getElement("fieldData")
                    last_price = fieldData.getElementAsFloat("PX_LAST")
                    volume = fieldData.getElementAsInteger("VOLUME")
                    data.append({
                        'ticker': ticker,
                        'last_price': last_price,
                        'volume': volume,
                        'timestamp': datetime.now() - timedelta(minutes=15)  # Adjust timestamp
                    })
        if ev.eventType() == blpapi.Event.RESPONSE:
            break
    return data

# Function to fetch economic indicators from Bloomberg DAPI API
def fetch_economic_indicators():
    session = blpapi.Session()
    session.start()
    session.openService("//blp/refdata")
    service = session.getService("//blp/refdata")

    request = service.createRequest("ReferenceDataRequest")

    # Assuming these are the Bloomberg tickers for economic indicators
    economic_tickers = {
        'interest_rate': 'IRATE INDEX',
        'unemployment_rate': 'UNEMP INDEX',
        'inflation_rate': 'INFL INDEX',
        'gdp_growth_rate': 'GDP INDEX'
    }

    for ticker in economic_tickers.values():
        request.append("securities", ticker)

    request.append("fields", "PX_LAST")

    session.sendRequest(request)

    indicators = {}
    while True:
        ev = session.nextEvent(500)
        for msg in ev:
            if ev.eventType() in [blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE]:
                securityDataArray = msg.getElement("securityData")
                for securityData in securityDataArray.values():
                    ticker = securityData.getElementAsString("security")
                    fieldData = securityData.getElement("fieldData")
                    if fieldData.hasElement("PX_LAST"):
                        value = fieldData.getElementAsFloat("PX_LAST")
                    else:
                        value = None  # Handle the case where the field is not available
                    for key, val in economic_tickers.items():
                        if val == ticker:
                            indicators[key] = value
        if ev.eventType() == blpapi.Event.RESPONSE:
            break
    return indicators

# Function to calculate RSI and MACD
def calculate_indicators(data):
    df = pd.DataFrame(data)
    df['rsi'] = ta.momentum.RSIIndicator(df['last_price']).rsi()
    macd = ta.trend.MACD(df['last_price'])
    df['macd'] = macd.macd()
    df['macd_signal'] = macd.macd_signal()
    result = df.to_dict('records')
    return result

# Function to ensure the table exists
def ensure_table_exists(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ticker VARCHAR(20),
        last_price FLOAT,
        volume INT,
        timestamp DATETIME,
        rsi FLOAT,
        macd FLOAT,
        macd_signal FLOAT,
        interest_rate FLOAT,
        unemployment_rate FLOAT,
        inflation_rate FLOAT,
        gdp_growth_rate FLOAT
    )
    """
    cur = conn.cursor()
    cur.execute(create_table_query)
    conn.commit()
    cur.close()

# Function to save data to MySQL
def save_to_mysql(data):
    conn = None
    try:
        # Connect to MySQL database
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        # Ensure the table exists
        ensure_table_exists(conn)
        
        cur = conn.cursor()
        
        # Define the SQL query to insert data
        insert_query = """
        INSERT INTO stock_data (ticker, last_price, volume, timestamp, rsi, macd, macd_signal, interest_rate, unemployment_rate, inflation_rate, gdp_growth_rate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Prepare data for insertion
        values = [
            (
                record['ticker'],
                record['last_price'],
                record['volume'],
                record['timestamp'],
                record['rsi'],
                record['macd'],
                record['macd_signal'],
                record.get('interest_rate'),
                record.get('unemployment_rate'),
                record.get('inflation_rate'),
                record.get('gdp_growth_rate')
            )
            for record in data
        ]
        
        # Execute the insert query for each record
        cur.executemany(insert_query, values)
        
        # Commit the transaction
        conn.commit()
        
        # Close communication with the MySQL database
        cur.close()
        print("Data saved to MySQL.")
    except Exception as error:
        print(f"Error saving data to MySQL: {error}")
    finally:
        if conn is not None:
            conn.close()

# Function to check if current time is within trading hours on weekdays
def is_trading_hours():
    now = datetime.now()
    is_weekday = now.weekday() < 5  # Monday to Friday are 0 to 4
    is_trading_time = dt_time(9, 30) <= now.time() <= dt_time(16, 0)
    return is_weekday and is_trading_time

# Main loop to continuously fetch and save data during trading hours on weekdays
def main():
    if not check_bloomberg_api():
        print("Bloomberg API is not available. Exiting...")
        return

    try:
        while True:
            if is_trading_hours():
                stock_data = fetch_stock_data_from_bloomberg()
                economic_indicators = fetch_economic_indicators()
                stock_data_with_indicators = calculate_indicators(stock_data)
                for record in stock_data_with_indicators:
                    record.update(economic_indicators)
                save_to_mysql(stock_data_with_indicators)
                print("Data fetched and saved.")
            else:
                print("Outside trading hours or it's a weekend. Waiting...")
            # Wait for a specified interval before checking again
            time.sleep(60)
    except KeyboardInterrupt:
        print("Script terminated by user.")

if __name__ == '__main__':
    main()
