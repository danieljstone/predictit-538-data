#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 18 14:40:20 2020

@author: danielstone
"""
import pandas_read_xml as pdx
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

"""
Instructions about how to push a dataframe to a sheet in a Google Docs Workbook can be found at https://gspread.readthedocs.io/en/latest/oauth2.html
For the sake of convenience, I use the same names for dataframes and worksheets.  I also have a sheet in the same workbook that includes information about all PredicIt Markets
"""

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name("KEYFILE See Instructions", scope)

gc = gspread.authorize(credentials)
gsheet=gc.open("Predictit Data")


def get_df_name(df):
    name =[x for x in globals() if globals()[x] is df][0]
    return name

def clean(df): #google sheets won't accept na/datetime values
    df.fillna('', inplace=True)
    df.reset_index(inplace=True)
    if "date" in df.columns:
        try:
            df["date"]=df["date"].dt.strftime('%B %d, %Y')
        except: 
            pass
        try:
            df["date"]=df['date'].astype(str)
        except: 
            pass 
        
def update(sheet):
    clean(sheet)
    name=get_df_name(sheet)
    gsheet.worksheet(name).update([sheet.columns.values.tolist()] + sheet.values.tolist())


predictitjson=pd.read_json("https://www.predictit.org/api/marketdata/all/")

allpredictit=pdx.fully_flatten(predictitjson)


presidentialdata=allpredictit[allpredictit['markets|name'].str.contains("presidential")]

stateraces=presidentialdata[presidentialdata['markets|name'].str.contains("Which party will win")]

contractinfocolumns=['markets|contracts|lastTradePrice',
       'markets|contracts|bestBuyYesCost',
       'markets|contracts|bestBuyNoCost',
       'markets|contracts|bestSellYesCost',
       'markets|contracts|bestSellNoCost',
       'markets|contracts|lastClosePrice']

statepivot=pd.pivot_table(stateraces,index="markets|name",columns='markets|contracts|name',values=contractinfocolumns)

#cleaning up column names
statepivot.columns=statepivot.columns.map('{0[1]}_{0[0]}'.format)
statepivot.columns=statepivot.columns.str.replace("|","").str.replace("marketscontracts","")

statepivot=statepivot.reindex(sorted(statepivot.columns), axis=1) #ordering by party


statepivot=statepivot[list(statepivot.columns)[-6:]+list(statepivot.columns)[:-6]]  #shifting third parties to the right

statepivot["state"]=(statepivot.index.str.split().str[4]+" "+statepivot.index.str.split().str[5]).str.replace(" in","").str.replace("the ","")

statepivot["state"]=statepivot["state"].str.replace("01","1").str.replace("02","2").str.replace("03","3").replace("DC","District of Columbia")
statepivot=statepivot[list(statepivot.columns)[-1:]+list(statepivot.columns)[:-1]] 


statepivot=statepivot.iloc[:-2,:-12]



"""
538 DATA
"""
#from https://github.com/fivethirtyeight/data/tree/master/election-forecasts-2020

latest538predictions=pd.read_csv("https://projects.fivethirtyeight.com/2020-general-data/presidential_state_toplines_2020.csv")

latest538predictions=latest538predictions[latest538predictions["modeldate"]==latest538predictions["modeldate"][0]]


"""
MERGING
"""

statepresidential=statepivot.merge(latest538predictions,how="left",left_on="state",right_on="state")
statepresidential=statepresidential.set_index("state")

"""
Uploading
"""

update(allpredictit)
update(statepresidential)
