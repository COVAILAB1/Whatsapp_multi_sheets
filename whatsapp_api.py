import gspread
import pandas as pd
import time

# Replace with your Google Sheet URL
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1wKW-b7XStIjOZFoB6lDHKa-n0koHsjxV_AfeYgbbpx4/edit?gid=0#gid=0'
gc = gspread.service_account(filename='Credentials.json')

# Open the spreadsheet by URL
spreadsheet = gc.open_by_url(SHEET_URL)

# Get all worksheet objects from the spreadsheet
worksheets = spreadsheet.worksheets()
if worksheets:
    first_worksheet = worksheets[0]
    rows = first_worksheet.get_all_records()

    # Print first 5 rows
    print(rows[:5])

    print('==============================')
    # Convert to DataFrame
    df = pd.DataFrame(rows)
    print(df.head())
else:
    print(f"No worksheets found in the spreadsheet with ID {SHEET_ID}.")