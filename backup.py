import sys,argparse,os,subprocess
from pathlib import Path
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import pandas as pd
from get_proc import get_config,get_last_backup

def update_history_file(btype,dirpath,bhistory):
    now = datetime.today().strftime('%Y-%m-%d')
    day_today = datetime.today().strftime('%A')

    backup_data = {'Backup_Date': [now],
                    'Backup_Type': [btype],
                    'Backup_Directory': [dirpath]}
    df = pd.DataFrame(backup_data)
        
    if os.path.exists(bhistory):
        print(f"\n===== updating backup history catalog ======\n")
        insert_history = df.to_csv(bhistory,header=False,index=False,mode='a')
    else:
        print(f"\n===== backup file history not found, creating backup file history now : {bhistory} =====")
        create_file = df.to_csv(bhistory,index=False,mode='a')
        

def check_backup_type():
        parser = argparse.ArgumentParser(prog='database backup',description='python script for database backup using percona xtrabackup. this script can do full and incremental backup with specify the backup type option')
        parser.add_argument('-t','--backup-type',required=True, help="backup type accepted value only incremental or full")
        parser.add_argument('-e','--env-file',required=True, help="env file for running backup")
        parser.add_argument('-f','--backup-history',required=True, help="backup history catalog file")
        args = vars(parser.parse_args())
        btype = args['backup_type']
        host = 'localhost'

        if btype == 'full':
            print('===== processing full backup =====\n')
            full_backup(args['backup_type'],args['env_file'],args['backup_history'])
            backup_retention(args['backup_history'],host,args['env_file'],retention=8)
        elif btype == 'incremental':
            print('===== processing incremental backup =====\n')
            last_backup_dir = get_last_backup(args['backup_history'])
            incremental_backup(last_backup_dir,args['backup_type'],args['env_file'],args['backup_history'])
        else:
             print("backup tyupe not found")


def check_backup_directory(dirpath,date):
        backup_dir = Path(f'{dirpath}{date}')
        try:
           os.makedirs(backup_dir)
           print(f"===== backup directory created: {backup_dir} =====\n")
        except Exception as error:
           print("backup failed:",error)
           sys.exit(1)

def backup_retention(bhistory,inshost,envfile,retention=7):
    data = pd.read_csv(Path(bhistory))
    try:
        load_dotenv(Path(envfile))
        backup_dir = list(os.environ['BACKUP_DIR'].split(","))
        host = list(os.environ['HOST'].split(","))
        data = pd.DataFrame(list(zip(host,backup_dir)),columns=['host','backup_dir'])
        new_data = data[data['host'] == inshost]
        bdir = new_data['backup_dir']
        for x in bdir:
             entries = os.listdir(x)
             for entry in entries:
                  full_path = os.path.join(x,entry)
                  dir_creation_time = datetime.fromtimestamp(os.path.getctime(full_path))
                  age = datetime.now() - dir_creation_time
                  if age.days > retention:
                       #shutil.rmtree(full_path)
                       print(f"Deleted : {full_path} (age : {age.days} days)")
    except Exception as error:
        print(error)
    
    try:
        today = datetime.now().date()
        data = pd.read_csv(Path(bhistory))
        data['Backup_Date'] = pd.to_datetime(data['Backup_Date'])
        retention_data = today - timedelta(days=retention)
        new_data = data[(data['Backup_Date'] >= pd.to_datetime(retention_data)) & (data['Backup_Date'] <= pd.to_datetime(today))]
        update_history = new_data.to_csv(bhistory,index=False)
    except Exception as error:
         print(error)


def full_backup(type,envpath,bhistory):
        bdir,username,today,passwd = get_config(type,envpath)
        check_backup_directory(bdir,today)
        backup_dir = f'{bdir}{today}'
        print('===== starting full backup =====\n')
        try:
          subprocess.run(["xtrabackup","--compress","--backup",f"--target-dir={bdir}{today}","-u",f"{username}",f"-p{passwd}"])
          print("\n===== Backup Completed ===== \n")
          update_history_file(type,backup_dir,bhistory)
        except Exception as error:
            print(error)

def incremental_backup(lbackup_dir,backup_type,envfile,bhistory):
        bdir,username,today,passwd = get_config(backup_type,envfile)
        check_backup_directory(bdir,today)
        backup_dir = f'{bdir}{today}'
        print('===== starting incremental backup =====\n')
       
        try:
            subprocess.run(["xtrabackup","--compress","--backup",f"--target-dir={backup_dir}",f"--incremental-basedir={lbackup_dir}","-u",f"{username}",f"-p{passwd}"])
            print("\n===== Backup Completed ===== \n")
            update_history_file(backup_type,backup_dir,bhistory)
        except Exception as error:
            print(error)

if __name__ == "__main__":
      check_backup_type()

