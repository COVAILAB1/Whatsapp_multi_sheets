import os
import pandas as pd
import requests
from time import sleep, time
from io import StringIO
import re
import pywhatkit as kit
import gspread
import logging
from openpyxl import load_workbook

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WhatsAppNotifier:
    def __init__(self):
        self.df = None
        self.sheet_length_tracker = self.load_sheet_lengths()
        self.last_check_time = 0
        self.check_interval = 30  
        if not os.path.exists("Document_db"):
            os.makedirs("Document_db")

    def read_csv_from_url(self, url):
        response = requests.get(url)
        response.raise_for_status()
        data = response.content.decode('utf-8')
        return pd.read_csv(StringIO(data))

    def download_file_from_google_drive(self, link, destination):
        if os.path.exists(destination):
            logger.info(f"File {destination} already exists. Skipping download.")
            return
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
            logger.info(f"File successfully downloaded to {destination}")
        except Exception as e:
            logger.error(f"Error saving file to {destination}: {e}")

    def xl(self):
        if time() - self.last_check_time < self.check_interval:
            return  
        self.last_check_time = time()

        logger.info("Checking for updates in the Google Sheet...")
        try:
            data_url = 'https://docs.google.com/spreadsheets/d/1wKW-b7XStIjOZFoB6lDHKa-n0koHsjxV_AfeYgbbpx4/edit?gid=0#gid=0'
           
            gc = gspread.service_account(filename='Credentials.json')

            spreadsheet = gc.open_by_url(data_url)
            
            worksheets = spreadsheet.worksheets()
            first_worksheet = worksheets[0]

            rows = first_worksheet.get_all_records()
            main_sheet_data = pd.DataFrame(rows)

            for idx, row in main_sheet_data.iterrows():
                sheet_id = row['sheet_id']
                field_values = {f"Field {i}": row.get(f"Field {i}") for i in range(1, 4) if pd.notna(row.get(f"Field {i}"))}
                if not field_values:
                    logger.info(f"No valid fields for sheet ID {sheet_id}. Skipping.")
                    continue
                
                msg = None
                image = False
                image_path = None

                if row['Field 4'] == "TEXT":
                    msg = row['message']

                elif row['Field 4'] == "IMAGE":
                    try:
                        file_name = row['Field 5']
                        link = row['Link']
                        destination = os.path.join("Document_db", file_name)
                        self.download_file_from_google_drive(link, destination)
                        image_path = os.path.join("Document_db", file_name)
                        image = True
                        msg = row.get('message', '')

                    except Exception as e:
                        logger.error(e)

                response_sheet_url = sheet_id
                self.process_sheet(response_sheet_url, sheet_id, field_values, msg, gc, image, image_path)
        except Exception as e:
            logger.error(f"Error occurred: {e}")

    def process_sheet(self, response_sheet_url, sheet_id, field_values, msg, gc, image, image_path):
        try:
            spreadsheet = gc.open_by_url(response_sheet_url)
            
            worksheets = spreadsheet.worksheets()
            first_worksheet = worksheets[0]

            rows = first_worksheet.get_all_records()
            response_sheet_data = pd.DataFrame(rows)

            current_length = len(response_sheet_data)
            logger.info(f"Current length of sheet {sheet_id}: {current_length}")
            if sheet_id in self.sheet_length_tracker:
                previous_length = self.sheet_length_tracker[sheet_id]
              
                if current_length > previous_length:
                    new_data_count = current_length - previous_length
                    self.data_retrieve(response_sheet_data, new_data_count, field_values, msg, image, image_path)
                    self.sheet_length_tracker[sheet_id] = current_length
                    self.save_sheet_lengths()
            else:
                self.sheet_length_tracker[sheet_id] = current_length
                self.save_sheet_lengths()

        except Exception as e:
            logger.error(f"Error occurred while processing sheet {sheet_id}: {e}")

    def data_retrieve(self, response_sheet_data, new_data_count, field_values, msg, image, image_path):
        logger.info(f"New data detected ({new_data_count} new entries). Sending WhatsApp messages...")
        field_data = list(field_values.values())
        logger.info(f"Field data: {field_data}")
        self.df = pd.DataFrame(response_sheet_data)

        for i in range(new_data_count):
            retrieved_data = {}
            for j in range(len(field_data)):
                key = j
                value = self.df.iloc[-(i+1)][field_data[j]]  
                retrieved_data[key] = value
            logger.info(f"Retrieved data: {retrieved_data}")
            if image:
                self.send_whatsapp_image(retrieved_data[1], image_path, retrieved_data, msg)
            else:
                self.send_whatsapp_messages(retrieved_data, msg)

    def send_whatsapp_messages(self, retrieved_data, msg):
        try:
            formatted_msg = msg.format(*[retrieved_data[key] for key in sorted(retrieved_data)])
            msg1 = "New Registration has been done \n Name:{0}, Course:{2}, Number:{1} in Registration."

            formatted_msg1 = msg1.format(*[retrieved_data[key] for key in sorted(retrieved_data)])
            phone_number = retrieved_data[1]  
            self.send_whatsapp_message(phone_number, formatted_msg, formatted_msg1)
        except Exception as e:
            logger.error(f"Failed to format or send message: {e}")

    def send_whatsapp_message(self, phone_number, message, msg1):
        try:
            kit.sendwhatmsg_instantly(f"+91{phone_number}", message, wait_time=25, tab_close=True, close_time=8)
            logger.info(f"Message sent to {phone_number}")
            sleep(5)  
            kit.sendwhatmsg_instantly('+919894954680', msg1, wait_time=20, tab_close=True, close_time=8)
            sleep(5)
            kit.sendwhatmsg_instantly('+917825933039', msg1, wait_time=20, tab_close=True, close_time=8)
        except Exception as e:
            logger.error(f"Failed to send message to {phone_number}: {e}")

    def send_whatsapp_image(self, phone_number, image_path, retrieved_data, msg):
        try:
            formatted_msg = msg.format(*[retrieved_data[key] for key in sorted(retrieved_data)])
            print(image_path)
            kit.sendwhats_image(f"+91{phone_number}", image_path, caption=formatted_msg, tab_close=True, close_time=5)
            logger.info(f"Image sent to {phone_number}")
            sleep(15)  
        except Exception as e:
            logger.error(f"Failed to send image to {phone_number}: {e}")

    def save_sheet_lengths(self):
        df = pd.DataFrame(list(self.sheet_length_tracker.items()), columns=['sheet_id', 'length'])
        df.to_excel('sheet_lengths.xlsx', index=False)
        logger.info("Sheet lengths saved to sheet_lengths.xlsx")

    def load_sheet_lengths(self):
        try:
            if os.path.exists('sheet_lengths.xlsx'):
                df = pd.read_excel('sheet_lengths.xlsx')
                return dict(zip(df.sheet_id, df.length))
        except Exception as e:
            logger.error(f"Failed to load sheet lengths: {e}")
        return {}

notifier = WhatsAppNotifier()
while True:
    notifier.xl()
    sleep(10)
