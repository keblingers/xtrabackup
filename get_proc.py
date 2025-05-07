from dotenv import load_dotenv
import os
from datetime import datetime,timedelta
from pathlib import Path
import pandas as pd

def get_config(host,btype,envfile):
        env_path = Path(envfile)
        load_dotenv(env_path)
        last_saturday,today_date,yesterday = get_date()
        host = list(os.environ['HOST'].split(","))
        backup_type = list(os.environ['BACKUP_TYPE'].split(","))
        backup_dir = list(os.environ['BACKUP_DIR'].split(","))
        user = list(os.environ['UNAME'].split(","))
        password = list(os.environ['PASSWORD'].split(","))
        last_backup = [last_saturday,yesterday]
        saturday = last_saturday
        today = today_date
        yday = yesterday
        df = pd.DataFrame(list(zip(host,backup_type,backup_dir,user,password)),columns=['host','backup_type','backup_dir','user','password'])
        data = df.query("host == @host and backup_type == @btype")
        iphost = data['host'].iloc[0]
        btype = data['backup_type'].iloc[0]
        bdir = data['backup_dir'].iloc[0]
        username = data['user'].iloc[0]
        passwd = data['password'].iloc[0]
        return iphost,bdir,username,today,passwd

def get_last_backup(bhistory):
    if os.path.exists(bhistory):
        data = pd.read_csv(bhistory)
        last_backup = data['Backup_Type'].iloc[-1]
        last_day = data['Backup_Date'].iloc[-1]
        last_backup_dir = data['Backup_Directory'].iloc[-1]
        return last_backup_dir
    else:
        print("backup file history not found")

def get_date():
        now = datetime.today()
        day_id = (now.weekday() + 1) % 7
        day_number = now - timedelta(7+day_id-6)
        last_saturday = day_number.strftime('%Y-%m-%d')
                        
        today_date = datetime.today().strftime("%Y-%m-%d")
        yday = now - timedelta(days=1)
        yesterday = yday.strftime('%Y-%m-%d')

        return last_saturday,today_date,yesterday

def get_last_lsn(basedir):
     checkpoint_file = Path(f"{basedir}/xtrabackup_checkpoints")
     with open(checkpoint_file,"r") as file:
          content = file.readlines()
          last_lsn = None
          for line in content:
               if line.startswith("last_lsn"):
                    last_lsn = line.split("=")[-1].strip()
                    break
          return last_lsn
     
def get_datadir(configfile):
     confile = Path(configfile)
     with open(confile,"r") as file:
        content = file.readlines()
        datadir = None
        for line in content:
             if line.startswith("datadir"):
                  datadir = line.split("=")[-1].strip()
                  break
        return datadir
     
def get_backup_history(bhistory,fbackup,lbackup=None):
    data = pd.read_csv(Path(bhistory))
    data['Backup_Date'] = pd.to_datetime(data['Backup_Date'])
    data.sort_values(by='Backup_Date',ascending=True,inplace=True)
    if lbackup == None:
        backup_history = data.loc[(data['Backup_Date'] == fbackup) & (data['Backup_Type'] == 'full')]
        return backup_history
    else:
        backup_history = data.loc[(data['Backup_Date'] >= fbackup) & (data['Backup_Date'] <= lbackup)]
        return backup_history
    
