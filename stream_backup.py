import sys, argparse, os, subprocess, shutil
from pathlib import Path
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import pandas as pd
from get_proc import get_config, get_last_backup, get_last_lsn


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
        parser.add_argument('-r','--retention',required=True,help="backup retention file before full backup")
        args = vars(parser.parse_args())
        btype = args['backup_type']

        if btype == 'full':
            print('===== processing full backup =====\n')
            full_backup(args['host'],args['env_file'],args['backup_history'],btype,args['retention'])

            print("\n===== Delete Expired Backup based on Retention Option =====\n")
        
        elif btype == 'incremental':
            print('===== processing incremental backup =====\n')
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
             
def full_backup(sourcehost,envpath,bhistory,btype,retention):
        host,bdir,username,today,passwd = get_config(sourcehost,btype,envpath)
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

