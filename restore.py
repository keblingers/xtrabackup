import sys
from pathlib import Path
import argparse
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import os
import pandas as pd
import subprocess

def get_backup_history(a,b):
    history_file = Path('./backup_history.csv')
    data = pd.read_csv(history_file)
    full_backup = data.query("Backup_Type == @a")
    incr_backup = data.query("Backup_Type == @b")
    incr_exl_last = incr_backup.head(len(incr_backup) - 1)
    incr_last = incr_backup.iloc[-1:]
    return full_backup, incr_exl_last,incr_last


def prepare_backup():
    full,increxl,incr_last = get_backup_history('full','incremental')
    fbackup = full['Backup_Directory'].iloc[0]
    lbackup = incr_last['Backup_Directory'].iloc[0]
    
    for x in increxl.index:
        bdir = increxl['Backup_Directory'][x]
        subprocess.run("xtrabackup","--prepare","--apply-log-only",f"target-dir={fbackup}",f"--incremental-dir{bdir}")
    return fbackup


def restore_backup(configfile,targetdir):
    subprocess.run("xtrabackup","--defaults-file={configfile}","--copy-back","--target-dir={targetdir}")

def run_script():
    parser = argparse.ArgumentParser(prog="Restore Backup",description="script to restore xtrabackup backup file mysql")
    parser.add_argument("-f","--history-file",required=True,help="this is the backup history file list in csv extension")
    parser.add_argument("-d","--target-dir",required=True,help="target dir where the backup will be restored")
    parser.add_argument("-c","--config-file",required=True,help="mysql config file where the backup will be restored")
    args = vars(parser.parse_args())
    print(args['history_file'])
    print(args['target_dir'])
    print(args['config_file'])




if __name__ == '__main__':
    run_script()