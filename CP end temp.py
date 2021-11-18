#!/usr/bin/env python3

from sqlalchemy import create_engine
import pyodbc
import urllib
import pandas as pd
import datetime
import random


# =============================================================================
# Read data from SQL datasource into dataframe
# =============================================================================

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
			WHERE DATEADD(d,DATEDIFF(d,0,[RECORDING_DATE]),0) > DATEADD(month,DATEDIFF(month,0,GETDATE())-6,0)
                 AND [CUSTOMER_CODE] IN ('10401005','10401523','10401009','10401057','10401207','10401522','10401028','10401087','10401054','10401510')
                 AND [ROASTER] = 'R2'
            GROUP BY
            	DATEADD(d,DATEDIFF(d,0,[RECORDING_DATE]),0)
            	,[ROASTER]
                ,[CUSTOMER_CODE]
			HAVING COUNT(*) >= 3
            ORDER BY
                DATEADD(d,DATEDIFF(d,0,[RECORDING_DATE]),0) ASC
                ,[CUSTOMER_CODE] ASC           	
                ,[ROASTER] ASC"""
# =============================================================================
#                 AND [CUSTOMER_CODE] = '10401005'
#                 AND [ROASTER] = 'R2'
# =============================================================================
# =============================================================================
# Variables
# =============================================================================
# change env variable below to switch between dev and prod SQL tables for inserts
env = 'dev'             # dev = test || cp = prod
# Read query into dataframe and create unique lists for iteration:
df = pd.read_sql(query, con)
roasters = df.ROASTER.unique()
recipes = df.Recipe.unique()
# Variables for inserting data into sql database:
params = urllib.parse.quote_plus('DRIVER={SQL Server Native Client 10.0};SERVER=sqlsrv04;DATABASE=BKI_Datastore;Trusted_Connection=yes')
engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
# Other variables:
now = datetime.datetime.now()
script_name = 'CP end temp.py'
execution_id = int(now.timestamp())
df_sign_recipes = pd.DataFrame()

# =============================================================================
# Define functions 
# =============================================================================
# Count the number of times a change has occured and add 1 to list
def diff_counter(diff_org, diff_new, counter_list):
    if diff_org > diff_new:
       counter_list[0] += 1
    if diff_org == diff_new:
        counter_list[1] += 1
    if diff_org < diff_new:
       counter_list[2] += 1
    counter_list[3] += 1


# Create list of of recipes with significant level of changes
def data_is_significant(greater_than_value, total_iterations, confidence_level):
    if total_iterations == 0:
        pass
    else:
        if greater_than_value / total_iterations >= confidence_level:
            return True


# Insert data into sql database
def insert_sql(dataframe, table_name, schema):
    dataframe.to_sql(table_name, con=engine, schema=schema, if_exists='append', index=False)
    
# Split dataframe in two
def dataset_split(input_dataframe):
    pass
    # While len(input_dataframe) > 10:
    #     df_temp = input_dataframe()

   
# =============================================================================
# Do initial analysis and bootstrapping
# =============================================================================
for recipe in recipes:
        for roaster in roasters:
            # Filter dataframe
            df_endtemp = df.loc[df['Recipe'] == recipe]
            df_endtemp = df_endtemp.loc[df['ROASTER'] == roaster]
            if len(df_endtemp) > 0:
                # Calculate mean for filtered dataframe
                avg_endtemp = df_endtemp['End temp'].mean()
                # Subtract mean from each datapoint and sum cumulative
                df_endtemp['End temp subtracted mean'] = df_endtemp['End temp'] - avg_endtemp
                df_endtemp['CumSum end temp diff'] = df_endtemp['End temp subtracted mean'].cumsum()
                # Find max and min values of end temp subtracted mean
                diff_endtemp_org = df_endtemp['CumSum end temp diff'].max() - df_endtemp['CumSum end temp diff'].min()
                
                # Create reordered dataframe, repeat calculations
                i = 0
                counter_list = [0,0,0,0] # Greater than, Equal, Less than, Total
    
                for i in range(1000):
                    df_temp = df_endtemp.sample(frac=1, replace=False, random_state=random.randint(1,999999))
                    df_temp['CumSum end temp diff'] = df_temp['End temp subtracted mean'].cumsum()
                    # Find max and min values of end temp subtracted mean
                    diff_temp_temp = df_temp['CumSum end temp diff'].max() - df_temp['CumSum end temp diff'].min()
                    # Add result of bootstrapping to counter
                    diff_counter(diff_endtemp_org, diff_temp_temp, counter_list)
                    
                    i += 1
                # Log initial analysis to sql after bootstrapping
                cp_count_sql = pd.DataFrame.from_dict({'Timestamp':[now], 'ExecutionId':[execution_id], 'No':[recipe],
                                'No id2':[roaster], 'OrgDiff':[diff_endtemp_org], 'Script':[script_name],
                                'CountGreater':counter_list[0] ,'CountEqual':counter_list[1], 
                                'CountLess':counter_list[2] ,'CountIterations':counter_list[3]}, orient='columns')
                insert_sql(cp_count_sql, 'ChangepointCounts',env)
                # Add recipe and roaster to dataframe, if significance level is high enough
                if data_is_significant(counter_list[0], counter_list[3], 0.95):
                    df_sign_recipes = df_sign_recipes.append({'Recipe': recipe, 'Roaster':roaster}, ignore_index=True)
                    # For development purposes only
                    df_endtemp.plot(x='Date',y='CumSum end temp diff', label=recipe)
    
    
                

# =============================================================================
# Dataframe for logging
# =============================================================================
df_log = pd.DataFrame(data={'Date':now, 'Event':script_name, 'Note':'Execution id: ' + str(execution_id)}, index=[0])

# =============================================================================
# Insert SQL
# =============================================================================
#insert_sql(df_log, 'Log', 'dbo')


print(df_sign_recipes)