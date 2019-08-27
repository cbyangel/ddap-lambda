#!/opt/anaconda3/envs/ddap/bin/python

import pandas as pd
import src.dbutil
import src.write_db
from main import main

if __name__ == '__main__':
    broad_dt = (pd.Timestamp.now() - pd.Timedelta('1 d')).strftime('%Y%m%d')
    src.write_db.delete_by_day_auto_table(broad_dt)
    src.write_db.delete_by_day_snapshot_table(broad_dt)
    main(broad_dt)
