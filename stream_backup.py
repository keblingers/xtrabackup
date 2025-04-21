import sys
from pathlib import Path
import argparse
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import os
import pandas as pd
import subprocess

def get_date():
        now = datetime.today()
        day_id = (now.weekday() + 1) % 7
        day_number = now - timedelta(7+day_id-6)
        last_saturday = day_number.strftime('%Y-%m-%d')
                        
        today_date = datetime.today().strftime("%Y-%m-%d")
        yday = now - timedelta(days=1)
        yesterday = yday.strftime('%Y-%m-%d')

        return last_saturday,today_date,yesterday

def get_config(host,b):
        env_path = Path(b)
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
        data = df.query("host == @host")
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

def update_history_file(btype,dirpath,bhistory):
    now = datetime.today().strftime('%Y-%m-%d')
    day_today = datetime.today().strftime('%A')

    backup_data = {'Backup_Date': [now],
                    'Backup_Type': [btype],
                    'Backup_Directory': [dirpath]}
    df = pd.DataFrame(backup_data)
        
    if os.path.exists(bhistory):
        insert_history = df.to_csv(bhistory,header=False,index=False,mode='a')
    else:
        print("backup file history not found, creating backup file history now")
        create_file = df.to_csv(bhistory,index=False,mode='a')
        

def check_backup_type():
        parser = argparse.ArgumentParser(prog='database backup',description='python script for database backup using percona xtrabackup. this script can do full and incremental backup with specify the backup type option')
        parser.add_argument('-t','--backup-type',required=True, help="backup type accepted value only incremental or full")
        parser.add_argument('-e','--env-file',required=True, help="env file for running backup")
        parser.add_argument('-f','--backup-history',required=True, help="backup history catalog file")
        parser.add_argument('-h','--host',required=True,help="source backup host")
        args = vars(parser.parse_args())
        btype = args['backup_type']

        if btype == 'full':
            print('===== processing full backup =====')
            full_backup(args['host'],args['env_file'],args['backup_history'],btype)
        
        elif btype == 'incremental':
            print('===== processing incremental backup =====')
            last_backup_dir = get_last_backup(args['backup_history'])
            incremental_backup(last_backup_dir,args['host'],btype,args['env_file'],args['backup_history'])

        else:
             print("backup tyupe not found")


def check_backup_directory(dirpath,date):
        backup_dir = Path(f'{dirpath}{date}')
        try:
           os.makedirs(backup_dir)
           print(f"===== backup directory created: {backup_dir} =====")
        except Exception as error:
           print("backup failed:",error)
           sys.exit(1)

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


def full_backup(sourcehost,envpath,bhistory,btype):
        host,bdir,username,today,passwd = get_config(sourcehost,envpath)
        check_backup_directory(bdir,today)
        backup_dir = f'{bdir}{today}'
        try:
          backup1 = subprocess.Popen(["ssh",f"{host}","xtrabackup","--compress","--backup","-u",f"{username}",f"-p{passwd}",f"--host={host}","--stream=xbstream",f"--target-dir={backup_dir}"],stdout=subprocess.PIPE)
          backup2 = subprocess.Popen(["xbstream","-x","-C",f"{backup_dir}"],stdin=backup1.stdout, stdout=subprocess.PIPE)
          backup = backup2.communicate()[0]
          update_history_file(btype,backup_dir,bhistory)

        except Exception as error:
            print(error)

def incremental_backup(lbackup_dir,shost,btype,envfile,bhistory):
        host,bdir,username,today,passwd = get_config(shost,envfile)
        check_backup_directory(bdir,today)
        backup_dir = f'{bdir}{today}'
        last_lsn = get_last_lsn(lbackup_dir)
        try:
            command1 = subprocess.Popen(["ssh",f"{host}","xtrabackup","--compress","--backup","-u",f"{username}",f"-p{passwd}",f"--host={host}","--stream=xbstream",f"--incremental-lsn={last_lsn}",f"target-dir={backup_dir}"],stdout=subprocess.PIPE)
            command2 = subprocess.Popen(["xbstream","-x","-C",f"{backup_dir}"])
            command1.stdout.close()
            command2.stdout.close()
            exct = command2.communicate()[0]
            update_history_file(btype,backup_dir,bhistory)
        except Exception as error:
            print(error)

if __name__ == "__main__":
      check_backup_type()

