from dotenv import load_dotenv
from sqlalchemy import create_engine
import os

from oauth2client.service_account import ServiceAccountCredentials
import gspread


load_dotenv()
CRED_PATH = os.getenv("CRED_PATH")

def auth_gspread():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(CRED_PATH, scope)

    gc = gspread.authorize(credentials)

    return gc