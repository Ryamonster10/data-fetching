from datetime import datetime, timedelta
import time
import blpapi
import pandas as pd
import ta  # Import the ta library for technical analysis

# Bloomberg DAPI API connection details
API_HOST = 'localhost'
API_PORT = 8194

# List of NDX Index tickers
NDX_TICKERS = [
    'AAPL US Equity', 'MSFT US Equity', 'GOOGL US Equity', 'AMZN US Equity', 'NVDA US Equity'
    # Add all other tickers in the NDX Index here
]

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

# Function to save data to CSV
def save_to_csv(data, filename='prices.csv'):
    df = pd.DataFrame(data)
    df.to_csv(filename, mode='a', header=not pd.io.common.file_exists(filename), index=False)

# Main loop to continuously fetch and save data
def main():
    if not check_bloomberg_api():
        print("Bloomberg API is not available. Exiting...")
        return

    try:
        while True:
            stock_data = fetch_stock_data_from_bloomberg()
            economic_indicators = fetch_economic_indicators()
            stock_data_with_indicators = calculate_indicators(stock_data)
            for record in stock_data_with_indicators:
                record.update(economic_indicators)
            save_to_csv(stock_data_with_indicators)
            # Wait for a specified interval before fetching data again
            time.sleep(60)
    except KeyboardInterrupt:
        print("Script terminated by user.")

if __name__ == '__main__':
    main()
