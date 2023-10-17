from datetime import datetime, date, timedelta
import pandas as pd
from dotenv import load_dotenv
import os
import subprocess

load_dotenv()
today = datetime.now().strftime("%A")
today_date = date.today()
yesterday = today_date - timedelta(days=1)

def get_config(day):
    backup_day = os.getenv('BACKUP_DAY').split(",")
    backup_type = os.getenv('BACKUP_TYPE').split(",")
    backup_date = f'{today_date},{yesterday}'.split(",")
    backup_dir = os.getenv('BACKUP_DIR').split(",")

    data = pd.DataFrame(list(zip(backup_type,backup_date,backup_dir,backup_day)),columns=['backup_type','backup_date','backup_dir','backup_day'])
    #print(f'\n{data}\n')
    df = data.query(f'backup_day.str.contains("{today}")')
    btype = df['backup_type'].iloc[0]
    bdate = df['backup_date'].iloc[0]
    bdir = df['backup_dir'].iloc[0]

    return btype,bdate,bdir

def check_backup_type():
    btype,bdate,bdir = get_config(today)
    if btype == 'full':
        full_backup(btype,bdate,bdir)
    elif btype == 'incremental':
        incremental_backup(btype,bdate,bdir)
    else:
        print('backup type not found')
    

def full_backup(type,date,dir):
    subprocess.run()
    
def incremental_backup(type,date,dir):
    subprocess.run()
     
     


check_backup_type()