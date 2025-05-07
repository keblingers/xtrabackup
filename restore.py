import sys,argparse,os,subprocess,shutil
from pathlib import Path
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import pandas as pd
from get_proc import get_datadir,get_backup_history

def stop_service():
    command = ["systemctl","is-active","mysqld"]
    process = subprocess.run(command, stdout=subprocess.PIPE)
    if process.returncode == 0:
        print('- mysql service is active -\n- stop service -')
        stop_cmd = ["systemctl","stop","mysqld"]
        exe_cmd = subprocess.run(stop_cmd,stdout=subprocess.PIPE)
    else:
        print('- mysql service is not running -')
    chk_svc = subprocess.Popen(["systemctl","is-active","mysqld"],stdout=subprocess.PIPE)
    stdout = chk_svc.stdout.read()
    print(stdout)
        
def start_service():
    command = ["systemctl","is-active","mysqld"]
    process = subprocess.run(command, stdout=subprocess.PIPE)
    if process.returncode == 0:
        print('- mysql service is active -')
    else:
        print('- mysql service is not running -\n- start service')
        start_cmd = ["systemctl","start","mysqld"]
        exe_start = subprocess.run(start_cmd,stdout=subprocess.PIPE)
    chk_svc = subprocess.Popen(["systemctl","is-active","mysqld"],stdout=subprocess.PIPE)
    stdout = chk_svc.stdout.read()
    print(stdout)

def check_directory(configfile):
    datadir = get_datadir(configfile)
    if not os.listdir(datadir):
        print(datadir,' is empty')
    else:
        print('delete list file:',datadir)
        for item in os.listdir(datadir):
            item_path = os.path.join(datadir,item)
            if os.path.isfile(item_path):
                os.remove(item_path)
                print('Deleted file: ',item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
                print('Deleted file: ',item_path)
            elif os.path.islink(item_path):
                os.unlink(item)
                print('Deleted file: ',item_path)

def prepare_backup(bhistory,confile,fbackup,lbackup=None,):
    if lbackup == None:
        data = get_backup_history(bhistory,fbackup)
        bdir = data['Backup_Directory'].iloc[0]
        decom_backup = subprocess.run(["xtrabackup","--decompress",f"--target-dir={bdir}"],stdout=subprocess.PIPE)
        prep_backup = subprocess.run(["xtrabackup","--prepare",f"--target-dir={bdir}"],stdout=subprocess.PIPE)
        
    else:
        data = get_backup_history(bhistory,fbackup,lbackup)
        full_row = data.loc[data['Backup_Type'] == 'full']
        yday = pd.to_datetime(datetime.strptime(lbackup,'%Y-%m-%d').date() - timedelta(days=1))
        data_min1 = data.loc[(data['Backup_Date'] >= fbackup) & (data['Backup_Date'] <= yday)]
        last_data = data.loc[(data['Backup_Date'] == lbackup)]
        
        for index,row in data_min1.iterrows():
            if row['Backup_Type'] == 'full':
                decom_backup = subprocess.run(["xtrabackup","--decompress",f"--target-dir={row['Backup_Directory']}"],stdout=subprocess.PIPE)
                prep_full = subprocess.run(["xtrabackup","--prepare","--apply-log-only",f"--target-dir={row['Backup_Directory']}"],stdout=subprocess.PIPE)
            elif row['Backup_Type'] == 'incremental':
                decom_backup = subprocess.run(["xtrabackup","--decompress",f"--target-dir={row['Backup_Directory']}"],stdout=subprocess.PIPE)
                prep_inc = subprocess.run(["xtrabackup","--prepare","--apply-log-only",f"--target-dir={full_row['Backup_Directory'].iloc[0]}",f"--incremental-dir={row['Backup_Directory']}"],stdout=subprocess.PIPE)
        decom_backup = subprocess.run(["xtrabackup","--decompress",f"--target-dir={last_data['Backup_Directory'].iloc[0]}"],stdout=subprocess.PIPE)
        prep_last = subprocess.run(["xtrabackup","--prepare",f"--target-dir={full_row['Backup_Directory'].iloc[0]}",f"--incremental-dir={last_data['Backup_Directory'].iloc[0]}"],stdout=subprocess.PIPE)

def restore_backup(bhistory,fbackup,configfile):
    data = get_backup_history(bhistory,fbackup)
    targetdir = data.loc[data['Backup_Date'] == fbackup]
    restor_db = subprocess.run(["xtrabackup",f"--defaults-file={configfile}","--copy-back",f"--target-dir={targetdir['Backup_Directory'].iloc[0]}"],stdout=subprocess.PIPE)

def change_file_permission(configfile):
    datadir = get_datadir(configfile)
    chgown = subprocess.run(["chown","-R","mysql:mysql",f"{datadir}"])
    print(chgown)

def run_script():
    parser = argparse.ArgumentParser(prog="Restore Backup",description="script to restore xtrabackup backup file mysql")
    parser.add_argument("-b","--history-file",required=True,help="this is the backup history file list in csv extension")
    parser.add_argument("-c","--config-file",required=True,help="mysql config file where the backup will be restored")
    parser.add_argument("-f","--first-backup",required=True,help="date of first backup that want to be restored must be full backup")
    parser.add_argument("-l","--last-backup",required=False,help="date of last backup that want to be restore should be incremental backup")
    args = vars(parser.parse_args())

    if args['last_backup'] == None:
        print("\n==== restore only full backup====\n")
        print("===== Check MySQL service and Stop Service ======")
        stop_service()
        print("===== Check datadir =====\n")
        check_directory(args['config_file'])
        print("===== Prepare Backup Files =====\n")
        prepare_backup(args['history_file'],args['config_file'],args['first_backup'])
        print("\n===== Restore Backup Files =====\n")
        restore_backup(args['history_file'],args['first_backup'],args['config_file'])
        print("\n===== change file permission =====\n")
        change_file_permission(args['config_file'])
        print("===== Check MySQL service and Start Service ======")
        start_service()
    else:
        print("\n==== restore full and incremental backup ====\n")
        print("===== Check MySQL service ======")
        stop_service()
        print("===== Check datadir =====\n")
        check_directory(args['config_file'])
        print("===== Prepare Backup Files =====\n")
        prepare_backup(args['history_file'],args['config_file'],args['first_backup'],args['last_backup'])
        print("\n===== Restore Backup Files =====\n")
        restore_backup(args['history_file'],args['first_backup'],args['config_file'])
        print("\n===== change file permission =====\n")
        change_file_permission(args['config_file'])
        print("===== Check MySQL service and Start Service ======")
        start_service()

if __name__ == '__main__':
    run_script()