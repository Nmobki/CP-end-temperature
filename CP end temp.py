#!/usr/bin/env python3

from sqlalchemy import create_engine
import pandas as pd
import pyodbc
import datetime


#Read login details for Probat
credentials = pd.read_excel(r'\\filsrv01\bki\11. Økonomi\04 - Controlling\NMO\22. Python\Credentials\Credentials.xlsx', header=0, index_col='Program').to_dict()
user = credentials['User']['Probat read']
password = credentials['Password']['Probat read']


# Define server connection and SQL query:
server = '192.168.125.161'
db = 'BKI_IMP_EXP'
con = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';UID=' + user + ';PWD=' + password)
query = """ SELECT
            	DATEADD(d,DATEDIFF(d,0,[RECORDING_DATE]),0) AS [Date]
            	,[ROASTER]
                ,[CUSTOMER_CODE] AS [Recipe]
                ,AVG([FINAL_TEMP_ROASTING] / 10.0) AS [End temp]
            FROM [dbo].[PRO_EXP_BATCH_DATA_ROASTER]
            GROUP BY
            	DATEADD(d,DATEDIFF(d,0,[RECORDING_DATE]),0)
            	,[ROASTER]
                ,[CUSTOMER_CODE] """

# Read query and create Profit calculation:
df = pd.read_sql(query, con)

print(df)


# Create timestamp and other variables
now = datetime.datetime.now()
scriptName = 'CP end temp.py'
executionId = int(now.timestamp())
tType = 'Change point detection, end temperature'
roasters = df.ROASTER.unique()
recipes = df.Recipe.unique()