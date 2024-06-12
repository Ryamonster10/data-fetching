import requests
import time
import os

# URL of the updated script
SCRIPT_URL = 'https://raw.githubusercontent.com/Ryamonster10/data-fetching/main/app.py'
LOCAL_SCRIPT_PATH = 'fetched_script.py'

def fetch_script():
    try:
        response = requests.get(SCRIPT_URL)
        if response.status_code == 200:
            with open(LOCAL_SCRIPT_PATH, 'w') as file:
                file.write(response.text)
            print("Fetched and saved the updated script.")
        else:
            print("Failed to fetch the updated script.")
    except Exception as e:
        print(f"An error occurred while fetching the script: {e}")

def execute_script():
    try:
        with open(LOCAL_SCRIPT_PATH) as file:
            exec(file.read(), globals())
    except Exception as e:
        print(f"An error occurred while executing the script: {e}")

def main():
    while True:
        try:
            fetch_script()
            execute_script()
            # Wait for a specified interval before fetching the script again
            time.sleep(60)
        except KeyboardInterrupt:
            print("Script terminated by user.")
            break
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == '__main__':
    main()
