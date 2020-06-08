#!/usr/bin/env python3

from sqlalchemy import create_engine
import pandas as pd


#Read login details for Probat
credentials = pd.read_excel(r'\\filsrv01\bki\11. Økonomi\04 - Controlling\NMO\22. Python\Credentials\Credentials.xlsx', header=0, index_col='Program').to_dict()
user = credentials['User']['Probat read']
password = credentials['Password']['Probat read']

