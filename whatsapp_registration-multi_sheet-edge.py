import os
import pandas as pd
import requests
from time import sleep, time
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.service import Service as EdgeService
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.edge.options import Options
import logging
import pickle

class WhatsAppNotifier:
    def __init__(self):
        self.df = None
        self.sheet_length_tracker = {}
        self.last_check_time = 0
        self.check_interval = 30
        if not os.path.exists("Downloaded"):
            os.makedirs("Downloaded")
        self.logger = self.setup_logging()
        self.driver = self.init_driver()
        self.load_cookies()

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        return logging.getLogger(__name__)

    def init_driver(self):
        try:
            edge_options = Options()
            edge_options.add_argument("--start-maximized")
            edge_options.add_argument("--log-level=3")
            edge_options.add_argument('--ignore-certificate-errors')
            
            edge_options.add_argument('--disable-features=EdgeUserTopicOnUrlProtobuf')  # Disable problematic feature


            driver_path = EdgeChromiumDriverManager().install()
            self.logger.info(f"Edge WebDriver installed at: {driver_path}")

            driver = webdriver.Edge(service=EdgeService(driver_path), options=edge_options)
            self.logger.info("Edge WebDriver initialized successfully.")
            return driver
        except Exception as e:
            self.logger.error(f"Failed to initialize Edge WebDriver: {e}")
            return None

    def save_cookies(self):
        with open("whatsapp_cookies.pkl", "wb") as file:
            pickle.dump(self.driver.get_cookies(), file)

    def load_cookies(self):
        if os.path.exists("whatsapp_cookies.pkl"):
            self.driver.get("https://web.whatsapp.com")
            with open("whatsapp_cookies.pkl", "rb") as file:
                cookies = pickle.load(file)
                for cookie in cookies:
                    self.driver.add_cookie(cookie)
            self.driver.refresh()
            self.logger.info("Cookies loaded and session restored.")

    def read_csv_from_url(self, url):
        response = requests.get(url)
        response.raise_for_status()
        data = response.content.decode('utf-8')
        return pd.read_csv(StringIO(data))

    def download_file_from_google_drive(self, link, destination):
        file_id = self.extract_file_id(link)
        if not file_id:
            raise ValueError("Invalid Google Drive link")
    
        URL = "https://drive.google.com/uc?export=download"

        with requests.Session() as session:
            response = session.get(URL, params={'id': file_id}, stream=True)
            token = self.get_confirm_token(response)

            if token:
                params = {'id': file_id, 'confirm': token}
                response = session.get(URL, params=params, stream=True)

            self.save_response_content(response, destination)

    def extract_file_id(self, link):
        match = re.search(r'/d/([a-zA-Z0-9_-]+)', link)
        return match.group(1) if match else None

    def get_confirm_token(self, response):
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                return value
        return None

    def save_response_content(self, response, destination):
        CHUNK_SIZE = 32768
        try:
            with open(destination, "wb") as f:
                for chunk in response.iter_content(CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
            print(f"File successfully downloaded to {destination}")
        except Exception as e:
            print(f"Error saving file to {destination}: {e}")

    def xl(self):
        if time() - self.last_check_time < self.check_interval:
            return
        self.last_check_time = time()

        print("Checking for updates in the Google Sheet...")
        try:
            data_url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSG3BB4D8sstgphi9RhWfueovJNXpzQRt8J82f4whTKZm1EbqAUXyRXgXRactFjXNJ1nfZVWkHqnXC-/pub?output=csv'
            main_sheet_data = self.read_csv_from_url(data_url)
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for idx, row in main_sheet_data.iterrows():
                    sheet_id = row['sheet_id']
                    field_values = {f"Field {i}": row.get(f"Field {i}") for i in range(1, 4) if pd.notna(row.get(f"Field {i}"))}
                    if not field_values:
                        print(f"No valid fields for sheet ID {sheet_id}. Skipping.")
                        continue

                    msg = None
                    if row['Field 4'] == "TEXT":
                        msg = row['message']
                    elif row['Field 4'] in ["IMAGE", "PDF"]:
                        try:
                            file_name = row['Field 5']
                            link = row['Link']
                            destination = os.path.join("Downloaded", file_name)
                            self.download_file_from_google_drive(link, destination)
                        except Exception as e:
                            print(e)

                    response_sheet_url = row['sheet_id']
                    futures.append(executor.submit(self.process_sheet, response_sheet_url, sheet_id, field_values, msg))

                for future in futures:
                    future.result()
        except Exception as e:
            print(f"Error occurred: {e}")

    def process_sheet(self, response_sheet_url, sheet_id, field_values, msg):
        try:
            response_sheet_data = self.read_csv_from_url(response_sheet_url)
            current_length = len(response_sheet_data)
            if sheet_id in self.sheet_length_tracker:
                previous_length = self.sheet_length_tracker[sheet_id]

                if current_length > previous_length:
                    new_data_count = current_length - previous_length
                    self.data_retrieve(response_sheet_data, new_data_count, field_values, msg)
                    self.sheet_length_tracker[sheet_id] = current_length
            else:
                self.sheet_length_tracker[sheet_id] = current_length

        except Exception as e:
            print(f"Error occurred while processing sheet {sheet_id}: {e}")

    def data_retrieve(self, response_sheet_data, new_data_count, field_values, msg):
        print(f"New data detected ({new_data_count} new entries). Sending WhatsApp messages...")
        field_data = list(field_values.values())
        print(field_data)
        self.df = pd.DataFrame(response_sheet_data)

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for i in range(new_data_count):
                retrieved_data = {}
                for j in range(len(field_data)):
                    key = j
                    value = self.df.iloc[-(i+1)][field_data[j]]
                    retrieved_data[key] = value
                print(retrieved_data)
                futures.append(executor.submit(self.send_whatsapp_messages, retrieved_data, msg))

            for future in futures:
                future.result()

    def send_whatsapp_messages(self, retrieved_data, msg):
        try:
            formatted_msg = msg.format(*[retrieved_data[key] for key in sorted(retrieved_data)])
            print(formatted_msg)
            phone_number = retrieved_data[1]
            self.send_whatsapp_message(phone_number, formatted_msg)
        except Exception as e:
            print(f"Failed to format or send message: {e}")

    def send_whatsapp_message(self, phone_number, message):
        for attempt in range(3):  # Retry mechanism
            try:
                self.driver.get(f'https://web.whatsapp.com/send?phone={phone_number}&text={message}')
                message_box = WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, '//div[@aria-label="Type a message"]'))
                )
                message_text = message
                message_box.send_keys(message_text)
                message_box.send_keys(Keys.ENTER)
                
                print(f"Message sent to {phone_number}")
                break
            except Exception as e:
                print(f"Failed to send message to {phone_number}: {e}")
               
notifier = WhatsAppNotifier()
while True:
    notifier.xl()
    sleep(10)
