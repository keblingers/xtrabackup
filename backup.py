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

def get_config(a):
        load_dotenv()
        last_saturday,today_date,yesterday = get_date()
        host = list(os.environ['HOST'].split(","))
        backup_type = list(os.environ['BACKUP_TYPE'].split(","))
        backup_dir = list(os.environ['BACKUP_DIR'].split(","))
        user = list(os.environ['USER'].split(","))
        password = list(os.environ['PASSWORD'].split(","))
        last_backup = [last_saturday,yesterday]
        saturday = last_saturday
        today = today_date
        yday = yesterday
        df = pd.DataFrame(list(zip(host,backup_type,backup_dir,user,password,last_backup)),columns=['host','backup_type','backup_dir','user','password','last_backup'])
        data = df.query("backup_type == @a")
        btype = data['backup_type'].iloc[0]
        bdir = data['backup_dir'].iloc[0]
        username = data['user'].iloc[0]
        lbackup = data['last_backup'].iloc[0]
        passwd = data['password'].iloc[0]
        return btype,bdir,username,today,lbackup,yesterday,passwd

def check_backup_type():
        parser = argparse.ArgumentParser(prog='database backup',description='python script for database backup using percona xtrabackup. this script can do full and incremental backup with specify the backup type option')
        parser.add_argument('-t','--backup-type',required=True, help="backup type accepted value only incremental or full")
        args = vars(parser.parse_args())

        if args['backup_type'] == 'full':
             full_backup()
        elif args['backup_type'] == 'incremental':
             incremental_backup()
        else:
             print("backup type not found")

def check_backup_directory(dirpath,date):
        backup_dir = Path(f'{dirpath}{date}')
        try:
           os.makedirs(backup_dir)
           print("backup directory created: ",backup_dir)
        except Exception as error:
           print("backup failed:",error)
           sys.exit(1)


def full_backup():
        btype,bdir,username,today,lbackup,yesterday,passwd = get_config('full')
        check_backup_directory(bdir,today)
        try:
          subprocess.run(["/root/tmp/percona-xtrabackup-8.0/bin/xtrabackup","--compress","--backup",f"--target-dir={bdir}{today}","-u","root",f"-p{passwd}"])
        except Exception as error:
            print(error)

def incremental_backup():
        btype,bdir,username,today,lbackup,yesterday,passwd = get_config('incremental')
        check_backup_directory(bdir,today)
        if today == 'Sunday':
             backuptype,backupdir,user,now,lastbackup,yday,passwd = get_config('full')
             try:
                 subprocess.run(["/root/tmp/percona-xtrabackup-8.0/bin/xtrabackup","--compress","--backup",f"--target-dir={bdir}{now}",f"--incremental-basedir={backupdir}{lastbackup}","-u",f"{user}",f"-p{passwd}"])
             except Exception as error:
                 print(error)
        else:
            try:
                subprocess.run(["/root/tmp/percona-xtrabackup-8.0/bin/xtrabackup","--compress","--backup",f"--target-dir={bdir}{now}",f"--incremental-basedir={bdir}{yesterday}","-u",f"{username}",f"-p{passwd}"])
            except Exception as error:
                print(error)




if __name__ == "__main__":
      check_backup_type()

