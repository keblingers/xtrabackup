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
    print(f'xtrabackup --prepare --apply-log-only --target-dir={fbackup}\n\n')
    
    for x in increxl.index:
        bdir = increxl['Backup_Directory'][x]
        print(f'xtrabackup --prepare --apply-log-only --target-dir={fbackup} --incremental-dir={bdir}')

    print(f'\n\nxtrabackup --prepare --target-dir={fbackup} --incremental-dir={lbackup}')




if __name__ == '__main__':
    prepare_backup()