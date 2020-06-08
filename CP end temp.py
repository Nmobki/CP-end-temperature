#!/usr/bin/env python3

from sqlalchemy import create_engine
import pandas as pd
import pyodbc
import datetime


# Read login details for Probat
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
			WHERE DATEADD(d,DATEDIFF(d,0,[RECORDING_DATE]),0) > DATEADD(d,DATEDIFF(d,0,GETDATE())-365,0)
                AND [CUSTOMER_CODE] = '10401005'
                AND [ROASTER] = 'R2'
            GROUP BY
            	DATEADD(d,DATEDIFF(d,0,[RECORDING_DATE]),0)
            	,[ROASTER]
                ,[CUSTOMER_CODE]
            ORDER BY
                DATEADD(d,DATEDIFF(d,0,[RECORDING_DATE]),0) ASC
                ,[CUSTOMER_CODE] ASC           	
                ,[ROASTER] ASC """

# Read query and create Profit calculation:
df = pd.read_sql(query, con)

print(df)


# Create timestamp and other variables
now = datetime.datetime.now()
scriptName = 'CP end temp.py'
executionId = int(now.timestamp())
sType = 'Change point detection, end temperature'
roasters = df.ROASTER.unique()
recipes = df.Recipe.unique()

for recipe in recipes:
        for roaster in roasters:
            # Filter dataframe
            dfEndTemp = df.loc[df['Recipe'] == recipe]
            dfEndTemp = dfEndTemp.loc[df['ROASTER'] == roaster]
            # Calculate mean for filtered dataframe
            endTempAvg = dfEndTemp['End temp'].mean()
            # Subtract mean from each datapoint and sum cumulative
            dfEndTemp['End temp subtracted mean'] = dfEndTemp['End temp'] - endTempAvg
            dfEndTemp['CumSum end temp diff'] = dfEndTemp['End temp subtracted mean'].cumsum()
            # Find max and min values of end temp subtracted mean
            endTempDiff = dfEndTemp['End temp subtracted mean'].max() - dfEndTemp['End temp subtracted mean'].min()
            

            endTempRows = dfEndTemp.index.max() #No. of rows in dataframe for iteration

            
            
            print(dfEndTemp)
            print(endTempAvg)
            print(endTempRows)
            print(endTempDiff)
            dfEndTemp.plot(x='Date',y='CumSum end temp diff')