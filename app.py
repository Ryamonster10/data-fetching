from datetime import datetime, timedelta, time as dt_time
import time
import blpapi
import pandas as pd
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
    session = blpapi.Session()
    if not session.start():
        print("Failed to start Bloomberg API session.")
        return False
    if not session.openService("//blp/refdata"):
        print("Failed to open Bloomberg API service.")
        return False
    return True

# Function to fetch RSI data from Bloomberg DAPI API
def fetch_rsi_data_from_bloomberg():
    session = blpapi.Session()
    session.start()
    session.openService("//blp/refdata")
    service = session.getService("//blp/refdata")

    request = service.createRequest("HistoricalDataRequest")
    for ticker in NDX_TICKERS:
        request.getElement("securities").appendValue(ticker)

    # Specify the fields we want
    request.getElement("fields").appendValue("RSI")

    # Set the overrides
    overrides = request.getElement("overrides")
    override1 = overrides.appendElement()
    override1.setElement("fieldId", "RSI")
    override1.setElement("value", "TAPeriod=14")
    override2 = overrides.appendElement()
    override2.setElement("fieldId", "DSClose")
    override2.setElement("value", "LAST_PRICE")
    override3 = overrides.appendElement()
    override3.setElement("fieldId", "Per")
    override3.setElement("value", "D")

    # Set the start and end dates for the request
    today = datetime.today().strftime("%Y%m%d")
    start_date = (datetime.today() - timedelta(days=30)).strftime("%Y%m%d")
    request.set("startDate", start_date)
    request.set("endDate", today)

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
                    fieldDataArray = securityData.getElement("fieldData")
                    for fieldData in fieldDataArray.values():
                        date = fieldData.getElementAsDatetime("date").date()
                        rsi = fieldData.getElementAsFloat("RSI")
                        data.append({
                            'ticker': ticker,
                            'date': date,
                            'rsi': rsi
                        })
        if ev.eventType() == blpapi.Event.RESPONSE:
            break
    return data

# Function to ensure the table exists
def ensure_table_exists(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ticker VARCHAR(20),
        date DATE,
        rsi FLOAT
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
        INSERT INTO stock_data (ticker, date, rsi)
        VALUES (%s, %s, %s)
        """
        
        # Prepare data for insertion
        values = [
            (
                record['ticker'],
                record['date'],
                record['rsi']
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
                rsi_data = fetch_rsi_data_from_bloomberg()
                save_to_mysql(rsi_data)
                print("Data fetched and saved.")
            else:
                print("Outside trading hours or it's a weekend. Waiting...")
            # Wait for a specified interval before checking again
            time.sleep(60)
    except KeyboardInterrupt:
        print("Script terminated by user.")

if __name__ == '__main__':
    main()
