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

def get_config(a,b):
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
        data = df.query("backup_type == @a")
        iphost = df.query['host'].iloc[0]
        btype = data['backup_type'].iloc[0]
        bdir = data['backup_dir'].iloc[0]
        username = data['user'].iloc[0]
        passwd = data['password'].iloc[0]
        return iphost,btype,bdir,username,today,yesterday,passwd

def get_last_backup(bhistory):
    #history_file = Path('./backup_history.csv')
    if os.path.exists(bhistory):
        data = pd.read_csv(bhistory)
        last_backup = data['Backup_Type'].iloc[-1]
        last_day = data['Backup_Date'].iloc[-1]
        return last_backup,last_day
    else:
        print("backup file history not found")

def update_history_file(btype,dirpath,bhistory):
    #history_file = Path('./backup_history.csv')
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
        args = vars(parser.parse_args())
        btype = args['backup_type']

        if btype == 'full':
            print('===== processing full backup =====')
            full_backup(args['backup_type'],args['env_file'],args['backup_history'])
        elif btype == 'incremental':
            print('===== processing incremental backup =====')
            last_backup,last_day = get_last_backup(args['backup_history'])
            incremental_backup(last_backup,last_day,args['backup_type'],args['env_file'],args['backup_history'])
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


def full_backup(type,envpath,bhistory):
        host,btype,bdir,username,today,yesterday,passwd = get_config(type,envpath)
        check_backup_directory(bdir,today)
        backup_dir = f'{bdir}{today}'
        try:
          #subprocess.run(["ssh",f"{host}","xtrabackup","--defaults-file=/etc/my.cnf","--login-path=local","--backup","--stream=xbstream",f"--target-dir={bdir}{today}","|","xbstream","-x","-C","/var/lib/data"])
          subprocess.run(["ssh",f"'{host}","xtrabackup","--compress","--backup","--stream=xbstream",f"--target-dir=/root/tmp/backup/{today}'","|","xbstream","-x","-C",f"/root/backup/{today}"])
          update_history_file(type,backup_dir,bhistory)
        except Exception as error:
            print(error)

def incremental_backup(type,last,backup,envfile,bhistory):
        btype,bdir,username,today,yesterday,passwd = get_config(backup,envfile)
        print(btype,bdir,username,today,yesterday,passwd)
        check_backup_directory(bdir,today)
        backup_dir = f'{bdir}{today}'
        if type == 'full':
             backuptype,backupdir,user,now,yday,passwd = get_config(type,envfile)
             try:
                 subprocess.run(["xtrabackup","--compress","--backup",f"--target-dir={bdir}{now}",f"--incremental-basedir={backupdir}{last}","-u",f"{user}",f"-p{passwd}"])
                 update_history_file(backup,backup_dir,bhistory)
             except Exception as error:
                 print(error)
        else:
            backuptype,backupdir,user,now,yday,passwd = get_config(type,envfile)
            try:
                subprocess.run(["xtrabackup","--compress","--backup",f"--target-dir={bdir}{now}",f"--incremental-basedir={bdir}{yesterday}","-u",f"{username}",f"-p{passwd}"])
                update_history_file(backup,backup_dir,bhistory)
            except Exception as error:
                print(error)

if __name__ == "__main__":
      check_backup_type()

