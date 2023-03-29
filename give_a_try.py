# -*- coding: utf-8 -*-
"""
Created on Mon Mar 27 22:14:34 2023

@author: vsviders
"""
import sys
sys.path.insert(0, r'C:\Users\VSviders\Documents\devs\E-test\ETEST_data\ParseModule')

import glob
import os

import ParseModule

path1 = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\TSMC\C00P69.00.txt"
path2 = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GMTest\KA03483-MAR24-2021.TXT"
path3 = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GF\WAT\7knu40973.000.csv.csv"

ll = []
for ind, path in enumerate(glob.glob(r'C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GF\WAT\*')):
    
    try:
        print(os.path.basename(path),ind)
        obj = ParseModule.GF2(path)
        date = obj.get_date()
        # df = obj.get_compelete_df()
        # check1 = tsmc.body_header_check()
        # check2 = tsmc.title_check()
        obj.file_check()
        ll.append([path, 'passed', date])
    
        
    
    except Exception as e:
        print(e)
        ll.append([path, e, date])
        # raise
    
# dfd = tsmc.parse_body_header()
    # kf = ParseModule.KeyFoundry(path2)
    # gf = ParseModule.GF(path3)


print('lolo')

import pandas as pd
ddf = pd.DataFrame(ll)



#%%
import pandas as pd

def convert_to_numeric(df):
   df = df.applymap(lambda x: x.replace(' ','') if isinstance(x,str) else x)
   df = df.replace('', np.nan)
   df = df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
   return df

def parse_body(lines):
    
    body = []

    for line in lines:
        
        line = line.strip()
        if line.startswith('WaferID'):
            temp = line.split(',')
            body.append(temp)
            
        if line[:2].strip().isdigit() or line[:1].strip().isdigit():
            temp = line.split(',')
            body.append(temp)
    
    body_df = pd.DataFrame(body)
    
    if body_df.empty:
        return body_df
    body_df = body_df.rename(columns=body_df.iloc[0]).drop(body_df.index[0])
    body_df.columns = [x.strip() for x in body_df.columns]
    
    body_df = body_df.drop(columns = ['Site_X','Site_Y', 'Pass/Fail'], axis=1)
    body_df = convert_to_numeric(body_df)
    body_df = body_df.dropna(how='all', axis=1)
    
    body_df = body_df.set_index(['WaferID', 'SiteID']).stack().reset_index()
    body_df.columns = ['wafer_id', 'structure_loc','param', 'value']
    body_df = convert_to_numeric(body_df)
    
    return body_df

def parse_body_header(self):
    
    body = []
    
    for line in self.lines:
        line = line.strip()

        if 'WaferID' in line or 'Unit' in line or 'SPEC HIGH' in line or 'SPEC LOW' in line:
            temp = line.split(',')
            body.append(temp)
        
    body_df = pd.DataFrame(body)
    if body_df.empty:
        return body_df
    
    body_df = convert_to_numeric(body_df)
    body_df = body_df.dropna(how='all', axis=1)
    body_df = body_df.rename(columns=body_df.iloc[0]).drop(body_df.index[0])
    body_df.columns = [x.strip() for x in body_df.columns]
    body_df = body_df.drop(columns = ['Site_X','Site_Y', 'Pass/Fail', 'WaferID', 'SiteID'], axis=1)
    body_df = body_df.T.reset_index()
    body_df.columns = ['param', 'unit', 'spec_high', 'spec_low']
    body_df = convert_to_numeric(body_df)
    
    return body_df
#%%
import numpy as np
import pandas as pd
p = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GF\WAT\7knv44041.000.csv"

with open(p, 'r') as f:
    lines = f.readlines()
    
    for line in lines:
        # line = line.strip()
        if line.startswith('Vendor Wafer Scribe ID'):
            print('we are here')
            line_ind = lines.index(line)
            data = pd.read_csv(p, skiprows = line_ind)
            break

data = data[data['SiteID'].notna()]
data = data.dropna(how='all', axis=1)
for x in data.columns:
    print(x)

data.columns = [x.strip() for x in data.columns]
data = data.drop(columns = ['Site_X','Site_Y', 'Pass/Fail', 'Vendor Wafer Scribe ID'], axis=1)

# data = convert_to_numeric(data)

data = data.set_index(['Wafer ID/Alias', 'SiteID']).stack().reset_index()
data.columns = ['wafer_id', 'structure_loc','param', 'value']
data = convert_to_numeric(data)

#%%
import ParseModule
p = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GF\WAT\7knv44041.000.csv"
print(os.path.basename(p),ind)
obj = ParseModule.GF2(p)
date = obj.get_date()

df1 = obj.parse_body_header()

#%%

with open(p, 'r') as f:
    lines = f.readlines()

body = []
for line in lines:
    line = line.strip()

    if 'SiteID' in line or 'Unit' in line or 'SPEC HIGH' in line or 'SPEC LOW' in line:
        temp = line.split(',')
        body.append(temp)
    
body_df = pd.DataFrame(body)
# if body_df.empty:
#     return body_df

# body_df = convert_to_numeric(body_df)
body_df = body_df.dropna(how='all', axis=1)
body_df = body_df.rename(columns=body_df.iloc[0]).drop(body_df.index[0])

# body_df.columns = [x.strip() for x in body_df.columns]

body_df = body_df.drop(columns = ['Site_X','Site_Y', 'Pass/Fail', 'Wafer ID/Alias', 'SiteID', 'Vendor Wafer Scribe ID'], axis=1)
body_df = body_df.T.reset_index()
body_df.columns = ['param', 'unit', 'spec_high', 'spec_low']
body_df = convert_to_numeric(body_df)



#%%

p = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GF\WAT\7knv44041.000.csv"
from datetime import datetime
with open(p, 'r') as f:
    lines = f.readlines()


title_params = {    'lot_id': None,
             'device_name': None,
             'process_name': None,
             'pcm_spec_name': None,
             'wafer_qty': None,
             'date': None
             }
   
for line in lines:
    
        if line.startswith('Lot ID'):      
            lot_id = line.split(',')[1].split('.')[0].strip()
            title_params['lot_id'] = lot_id
            
        if line.startswith('Customer Product Name'):         
            temp = line.split(',')[1].strip()
            title_params['device_name'] = temp
            
            
        if line.startswith('Timestamp (End)'):      
            temp = line.split(',')[1].strip()
            temp = datetime.strptime(temp, '%Y/%m/%d %H:%M:%S')
            title_params['date'] = temp
        
        if line.startswith('Wafer Count'):         
            temp = line.split(',')[1].strip()
            title_params['wafer_qty'] = temp
            
        if line.startswith('Technology'):         
            temp = line.split(',')[1].strip()
            title_params['process_name'] = temp

      


#%%
    
body = []

dodo = { 'WaferID': None,
        'Unit': None,
        'SPEC HIGH': None,
        'SPEC LOW': None
        }

for line in lines:
    line = line.strip()
    
    fofo = ['WaferID', 'Unit', 'SPEC HIGH', 'SPEC LOW' ]
    
    for fo in fofo:
        if fo in line:
            temp = line.split(',')
            body.append(temp)
    
    # if any(f in line for f in dodo.keys()):
        
    #     temp = line.split(',')
    #     body.append(temp)
    
if None in tuple(dodo.values()):
    print('df')

        
            

body_df = pd.DataFrame(body)
# body_df = convert_to_numeric(body_df)
    
# body = []

# for line in lines:
    
#     line = line.strip()
#     if line.startswith('WaferID'):
#         temp = line.split(',')
#         body.append(temp)
        
#     if line[:2].strip().isdigit() or line[:1].strip().isdigit():
#         temp = line.split(',')
#         body.append(temp)

# body_df = pd.DataFrame(body)
# if body_df.empty:
    # print('er lkdasjf')
# body_df = body_df.rename(columns=body_df.iloc[0]).drop(body_df.index[0])
# body_df.columns = [x.strip() for x in body_df.columns]

# body_df = body_df.drop(columns = ['Site_X','Site_Y', 'Pass/Fail'], axis=1)
# body_df = convert_to_numeric(body_df)
# body_df = body_df.dropna(how='all', axis=1)

# body_df = body_df.set_index(['WaferID', 'SiteID']).stack().reset_index()
# body_df.columns = ['wafer_id', 'structure_loc','param', 'value']
# body_df = convert_to_numeric(body_df)

#%%
# try:
p = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GF\WAT\KNOWLES_FAB7_ETEST_7KNV37946.020.csv"

tsmc = ParseModule.GF(p)

date = tsmc.get_date()
# tsmc.file_check()
# tsmc_df = tsmc.get_compelete_df()
# tsmc_df = tsmc.get_compelete_df()

