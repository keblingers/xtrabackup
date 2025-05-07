import sys,argparse,os,subprocess
from pathlib import Path
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import pandas as pd
from get_proc import get_datadir,get_backup_history

def check_service():
    command = ["systemctl","is-active","mysqld"]
    process = subprocess.run(command, capture_output=True, text=True, check=True)
    if process.returncode == 0:
        print('- mysql service is active -\n - shutting down service ')
        stop_cmd = ["systemctl","stop","mysql"]
        exe_cmd = subprocess.run(command,stdout=subprocess.PIPE)
    else:
        print('- mysql service is not running -')

def prepare_backup(bhistory,fbackup,lbackup=None,):
    #print(bhistory,fbackup,lbackup)
    #datadir = get_datadir(confile)
    if lbackup == None:
        data = get_backup_history(bhistory,fbackup)
        bdir = data['Backup_Directory'].iloc[0]
        #prep_backup = subprocess.run(["xtrabackup",f"--defaults-file={confile}","--prepare",f"--target-dir={bdir}"],stdout=subprocess.PIPE)
        #prep_backup = subprocess.run(["xtrabackup","--prepare",f"--target-dir={bdir}"])
        prep_backup = f'subprocess.run(["xtrabackup","--prepare",f"--target-dir={bdir}"])'
        print(prep_backup)
        
    else:
        data = get_backup_history(bhistory,fbackup,lbackup)
        full_row = data.loc[data['Backup_Type'] == 'full']
        yday = pd.to_datetime(datetime.strptime(lbackup,'%Y-%m-%d').date() - timedelta(days=1))
        data_min1 = data.loc[(data['Backup_Date'] >= fbackup) & (data['Backup_Date'] <= yday)]
        last_data = data.loc[(data['Backup_Date'] == lbackup)]
        
        for index,row in data_min1.iterrows():
            if row['Backup_Type'] == 'full':
                #prep_full = subprocess.run(["xtrabackup","--prepare","--apply-log-only",f"--target-dir={row['Backup_Directory']}"],stdout=subprocess.PIPE)
                prep_full = f'subprocess.run(["xtrabackup","--prepare","--apply-log-only",f"--target-dir={row['Backup_Directory']}"])'
                print(prep_full)
            elif row['Backup_Type'] == 'incremental':
                #prep_inc = subprocess.run(["xtrabackup","--prepare","--apply-log-only",f"--target-dir={full_row['Backup_Directory'].iloc[0]}",f"--incremental-dir={row['Backup_Directory']}"],stdout=subprocess.PIPE)
                prep_inc = f'subprocess.run(["xtrabackup","--prepare","--apply-log-only",f"--target-dir={full_row['Backup_Directory'].iloc[0]}",f"--incremental-dir={row['Backup_Directory']}"])'
                print(prep_inc)
        #prep_last = subprocess.run(["xtrabackup","--prepare",f"--target-dir={full_row['Backup_Directory'].iloc[0]}",f"--incremental-dir={last_data['Backup_Directory'].iloc[0]}"],stdout=subprocess.PIPE)
        prep_last = f'subprocess.run(["xtrabackup","--prepare",f"--target-dir={full_row['Backup_Directory'].iloc[0]}",f"--incremental-dir={last_data['Backup_Directory'].iloc[0]}"])'
        print(prep_last)

def restore_backup(bhistory,fbackup,configfile):
    data = get_backup_history(bhistory,fbackup)
    targetdir = data.loc[data['Backup_Date'] == fbackup]
    #print(targetdir)
    #restor_db = subprocess.run(["xtrabackup",f"--defaults-file={configfile}","--copy-back",f"--target-dir={targetdir['Backup_Dir'].iloc[0]}"],stdout=subprocess.PIPE)
    restor_db = f'subprocess.run(["xtrabackup",f"--defaults-file={configfile}","--copy-back",f"--target-dir={targetdir['Backup_Directory'].iloc[0]}"])'
    print(restor_db)

def change_file_permission(configfile):
    datadir = get_datadir(configfile)
    print(datadir)
    #chgown = subprocess.run(["chown","-R","mysql:mysql",f"{datadir}"])
    chgown = f'subprocess.run(["chown","-R","mysql:mysql",f"{datadir}"])'
    print(chgown)

def start_service():
    command = ["systemctl","start","mysql"]
    
    check_service = subprocess.run(chk_command, capture_output=True, text=True, check=True)
    if check_service.returncode == 0:
        print('service mysql is running')
    else:
        chk_command = ["systemctl","is-active","mysql"]
        process = subprocess.run(command,stdout=subprocess.PIPE)

def run_script():
    parser = argparse.ArgumentParser(prog="Restore Backup",description="script to restore xtrabackup backup file mysql")
    parser.add_argument("-b","--history-file",required=True,help="this is the backup history file list in csv extension")
    parser.add_argument("-c","--config-file",required=True,help="mysql config file where the backup will be restored")
    parser.add_argument("-f","--first-backup",required=True,help="date of first backup that want to be restored must be full backup")
    parser.add_argument("-l","--last-backup",required=False,help="date of last backup that want to be restore should be incremental backup")
    args = vars(parser.parse_args())

    #print(args['last_backup'])
    #targetdir = 

    if args['last_backup'] == None:
        print("\n==== restore only full backup====\n")
        print("===== Prepare Backup Files =====\n")
        prepare_backup(args['history_file'],args['first_backup'])
        print("\n===== Restore Backup Files =====\n")
        restore_backup(args['history_file'],args['first_backup'],args['config_file'])
        print("\n===== change file permission =====\n")
        change_file_permission(args['config_file'])
    else:
        print("\n==== restore full and incremental backup ====\n")
        print("===== Prepare Backup Files =====\n")
        prepare_backup(args['history_file'],args['first_backup'],args['last_backup'])
        print("\n===== Restore Backup Files =====\n")
        restore_backup(args['history_file'],args['first_backup'],args['config_file'])
        print("\n===== change file permission =====\n")
        change_file_permission(args['config_file'])

if __name__ == '__main__':
    run_script()