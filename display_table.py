from datetime import datetime, date, timedelta
import pandas as pd
from dotenv import load_dotenv
import os
import subprocess

load_dotenv()
today = datetime.now().strftime("%A")
today_date = date.today()
yesterday = today_date - timedelta(days=1)
full_date = (datetime.now()-timedelta(days=((datetime.now().isoweekday()+1)%7))).strftime('%Y-%m-%d')

def get_config():
    backup_day = os.getenv('BACKUP_DAY').split(",")
    backup_type = os.getenv('BACKUP_TYPE').split(",")
    backup_date = f'{full_date},{today_date}'.split(",")
    backup_dir = os.getenv('BACKUP_DIR').split(",")
    last_backup_date = f'{full_date},{yesterday}'.split(",")

    data = pd.DataFrame(list(zip(backup_type,backup_date,last_backup_date,backup_dir,backup_day)),columns=['backup_type','backup_date','last_backup_date','backup_dir','backup_day'])
    #print(f'\n{data}\n')
    print(data)
    # df = data.query(f'backup_day.str.contains("{today}")')
    # btype = df['backup_type'].iloc[0]
    # bdate = df['backup_date'].iloc[0]
    # bdir = df['backup_dir'].iloc[0]

    #return btype,bdate,bdir

get_config()