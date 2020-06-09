#!/usr/bin/env python3

from sqlalchemy import create_engine
import pandas as pd
import pyodbc
import datetime
import random

# Read login details for Probat
Credentials = pd.read_excel(r'\\filsrv01\bki\11. Økonomi\04 - Controlling\NMO\22. Python\Credentials\Credentials.xlsx', header=0, index_col='Program').to_dict()
User = Credentials['User']['Probat read']
Password = Credentials['Password']['Probat read']


# Define server connection and SQL query:
Server = '192.168.125.161'
Db = 'BKI_IMP_EXP'
Con = pyodbc.connect('DRIVER={SQL Server};SERVER=' + Server + ';DATABASE=' + Db + ';UID=' + User + ';PWD=' + Password)
Query = """ SELECT
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

# Read query into dataframe:
Df = pd.read_sql(Query, Con)

# Create timestamp and other variables
Now = datetime.datetime.now()
ScriptName = 'CP end temp.py'
ExecutionId = int(Now.timestamp())
S_Type = 'Change point detection, end temperature'
Roasters = Df.ROASTER.unique()
Recipes = Df.Recipe.unique()

# Lav en function der laver increment på dictionary med keys over, under, lig med
Count_Diff = {}

def diff_counter(dictionary, recipe, Diff_Org, Diff_New):
    dictionary[recipe]['Org greater']['Count'] = 0
    dictionary[recipe]['Equal']['Count'] = 0
    dictionary[recipe]['Org lower']['Count'] = 0
    dictionary[recipe]['Total']['Count'] = 0
    if Diff_Org > Diff_New:
        dictionary[recipe]['Org greater']['Count'] += 1
    if Diff_Org == Diff_New:
        dictionary[recipe]['Equal']['Count'] += 1
    if Diff_Org < Diff_New:
        dictionary[recipe]['Org lower']['Count'] += 1
    dictionary[recipe]['Total']['Count'] += 1
    

for Recipe in Recipes:
        for Roaster in Roasters:
            # Filter dataframe
            Df_EndTemp = Df.loc[Df['Recipe'] == Recipe]
            Df_EndTemp = Df_EndTemp.loc[Df['ROASTER'] == Roaster]
            # Calculate mean for filtered dataframe
            Avg_EndTemp = Df_EndTemp['End temp'].mean()
            # Subtract mean from each datapoint and sum cumulative
            Df_EndTemp['End temp subtracted mean'] = Df_EndTemp['End temp'] - Avg_EndTemp
            Df_EndTemp['CumSum end temp diff'] = Df_EndTemp['End temp subtracted mean'].cumsum()
            # Find max and min values of end temp subtracted mean
            Diff_EndTemp_Org = Df_EndTemp['CumSum end temp diff'].max() - Df_EndTemp['CumSum end temp diff'].min()
            
            # Create reordered dataframe, repeat calculations
            i = 0
            for i in range(1000):
                Df_Temp = Df_EndTemp.sample(frac=1, replace=False, random_state=random.randint(1,999999))
                Df_Temp['CumSum end temp diff'] = Df_Temp['End temp subtracted mean'].cumsum()
                # Find max and min values of end temp subtracted mean
                Diff_Temp_Temp = Df_Temp['CumSum end temp diff'].max() - Df_Temp['CumSum end temp diff'].min()
                diff_counter(Count_Diff, Recipe, Diff_EndTemp_Org, Diff_Temp_Temp)
                
                i += 1
                print(Count_Diff)
            
            endTempRows = Df_EndTemp.index.max() #No. of rows in dataframe for iteration


            print(Df_EndTemp)
            print(Avg_EndTemp)
            print(endTempRows)
            print(Diff_EndTemp_Org)
            print(Df_Temp)

            # For development purposes only
            Df_EndTemp.plot(x='Date',y='CumSum end temp diff')