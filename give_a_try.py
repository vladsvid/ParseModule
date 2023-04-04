# -*- coding: utf-8 -*-
"""
Created on Mon Mar 27 22:14:34 2023

@author: vsviders
"""
import sys
sys.path.insert(0, r'C:\Users\VSviders\Documents\devs\E-test\ETEST_data\ParseModule')

import glob
import os
import re


from ParseModule import TSMC, KeyFoundry, GF, GF2

from sqlalchemy import create_engine

from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy import exc
import traceback
import pandas as pd
import numpy as np

def convert_to_numeric(df):
   df = df.applymap(lambda x: x.strip() if isinstance(x,str) else x)
   df = df.replace('', np.nan)
   df = df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
   return df


path = r'C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GF\WAT\7knw38150.000.csv'
path = r'C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GMTest\KA01022-MAR15-2021.TXT'
# path = r'C:\Users\VSviders\Documents\devs\E-test\ETEST_data\TSMC\C00P54.00.txt'
# path = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GMTest\KA00776-MAR08-2021.TXT"
obj = KeyFoundry(path)

date = obj.get_date()
print(date)

title_params = obj.parse_title()

body_he = obj.parse_body_header()



# # dev_name = title_params.get('device_name')
# # # # header = obj.title_to_dataframe()
df = obj.get_compelete_df(check=True)

#%% KF


params_header = pd.DataFrame()
body_header = [['PARAMETER', 'param'], ['SPEC LOW', 'spec_low'],['SPEC HIGH', 'spec_high'], ['WAFER ID / UNIT', 'unit']]
drop_list = ['PARAMETER']
body = []

for line in lines:
    line = line.strip()
    for bd in body_header:
        if line.startswith(bd[0]):
            temp = re.sub("\s\s+", "|", line)
            temp = temp.split('|')
            temp.insert(0, bd[1])
            body.append(temp)




params_header = pd.DataFrame(body).T
params_header = params_header.rename(columns=params_header.iloc[0]).drop(params_header.index[0])
params_header = params_header[params_header['param'].isin(drop_list) == False]
params_header = convert_to_numeric(params_header)
# params_header = params_header.dropna()
    
    



    
    #%%
import re
    
string = '''
Lot ID,7KNW52763.000
Timestamp (Start),2022/03/13 12:26:23
Timestamp (End),2022/03/13 19:21:06
Fab,7
Technology,cmos8lp
Product ID,FREELANDER.05
Customer Product Name,401-094-18
Test Program,EN13G,EN15G,EZ11B,EZ51A,EZ64A,EZ81A,EZ82A,
Equipment Id,yets709
Parameter Count,63
Temperature,25
Flat Orientation,DOWN,DOWN,DOWN,DOWN,DOWN,DOWN,DOWN,
Wafer Count,25
Test Level (Metal Type),LT '''   

#%%
import pandas as pd
  
params_header = pd.DataFrame()



body_header = [['ID','unit'], ['SPEC HI', 'spec_high'], ['SPEC LO','spec_low'], ['WAF','param']]

drop_list = ['WAF', 'SITE']



for b in sliced_data[:1]:
    print(1)
    body = []
    for line in sliced_data[0]:
        line = line.strip()
        for bd in body_header:
            if line.startswith(bd[0]):
                print(bd)
                line = line.replace('SPEC HI', 'SPEC_HI  NULL')
                line = line.replace('SPEC LO', 'SPEC_LO  NULL')
                temp = re.sub('\s+', '|', line)
                temp = temp.split('|')
                temp.insert(0, bd[1])
                body.append(temp)

              
            # Remove columns which not needed in data stack
        temp_df = pd.DataFrame(body).T
        temp_df = temp_df.rename(columns=temp_df.iloc[0,:]).drop(temp_df.index[0])
        temp_df = temp_df[temp_df['param'].isin(drop_list) == False]
        #     
        #     # Transpose and rename dataframe
        #     temp_df = temp_df.T.reset_index()
        #     temp_df = temp_df.rename(columns=temp_df.iloc[0]).drop(temp_df.index[0])
        #     temp_df = temp_df.rename(columns ={'ID' : 'unit', 'SPEC_LO': 'spec_low', 'SPEC_HI': 'spec_high', 'WAF': 'param'})
        #     params_header = pd.concat([params_header, temp_df])
        # params_header = convert_to_numeric(params_header)
        





#%%

with open(p, 'r') as f:
    lines = f.readlines()
    
    
    title_params = self.get_title_params_init('TSMC')

    with open(self.filepath, 'r', encoding = 'utf8') as f:
        string = f.read()

    patterns = {'device_name':  re.compile(r"{}\s*:([\w-]+)".format('TYPE NO')),
          'process_name': re.compile(r"{}\s*:([\w-]+)".format('PROCESS')),
          'wafer_qty': re.compile(r"{}\s*:([0-9]+)".format('QTY')),
          'date': re.compile(r"{}\s*:([0-9/]+)".format('DATE')),
          'pcm_spec_name': re.compile(r"{}\s*:([\w-]+)".format('PCM SPEC')),
          'lot_id': re.compile(r"{}\s*:([\w]+)".format('LOT ID'))}

    for k, v in patterns.items():
        match = patterns[k].search(string)
        if match:
            res = match.group(1)
        else:
            res = match
        title_params[k] = res
        
    return title_params

#%%

engine = create_engine("mysql://root:root@localhost/my_db")
def load_data(data, sql_table, engine):
    
    try:
        data.to_sql(sql_table, engine, if_exists='append', index=False)
        print('all good')
 
    # except exc.OperationalError:
    #     print('Different table schema')
    #     # meta = sqlalchemy.MetaData(bind=engine)
    #     # sqlalchemy.MetaData.reflect(meta)
    #     # db_table = meta.tables[sql_table]
    #     # df_cols = list(df.columns)
    #     # db_cols = db_table.columns.keys()
    #     # diff = list(set(df_cols) - set(db_cols))
    #     # print(diff)
    #     df_db = pd.read_sql_table(sql_table, engine.connect())
    #     ddf = pd.concat([data, df_db])
    #     ddf.to_sql(sql_table, engine, if_exists='replace', index=False)
    #     print('Table updated')
        
    # except exc.DataError as e:
    #     shutil.copy()
    #     print(e)
    
    except Exception:
        print(traceback.format_exc())
        raise

path1 = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\TSMC\C00P69.00.txt"
path2 = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GMTest\KA03483-MAR24-2021.TXT"
path3 = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GF\WAT\7knu40973.000.csv.csv"

ll = []
for ind, path in enumerate(glob.glob(r'C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GF\WAT\*')):
    
    try:
        print(os.path.basename(path),ind)
        obj = GF2(path)
        date = obj.get_date()
        # df = obj.get_compelete_df()
        obj.file_check()
        df_header = obj.title_to_dataframe()
        # check1 = tsmc.body_header_check()
        # check2 = tsmc.title_check()
        
        ll.append([path, 'passed', date])
        
        load_data(df_header, 'gf_header' , engine)
        # load_data(df, 'gf_data' , engine)
    
        
    
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



ddf.to_csv(r'C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GF_errors.csv', index=False)
#%%

ddf.columns = ['path', 'ifPassed', 'date']

ddf_filterred = ddf[ddf['ifPassed'] != 'passed']








#%%
import pandas as pd
import numpy as np




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
data = data.drop(columns = ['Pass/Fail', 'Vendor Wafer Scribe ID'], axis=1)

data = data.drop(data.filter(regex='Site_').columns, axis=1)

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
df = obj.get_compelete_df()

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
body_df = body_df.dropna(how='all')



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

#%% TSMC
import re

p = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GF\WAT\7knw52763.000.csv"

with open(p, 'r') as f:
    string = f.read()
    
    
fin ={'lot_id': None,
             'device_name': None,
             'process_name': None,
             'pcm_spec_name': None,
             'wafer_qty': None,
             'd': None
             }


dd = {'device_name':  re.compile(r"{}\s*,\s*([\w-]+)".format('Customer Product Name')),
      'process_name': re.compile(r"{}\s*,\s*([\w]+)".format('Technology')),
      'wafer_qty':    re.compile(r"{}\s*,\s*([0-9]+)".format('Wafer Count')),
      'lot_id':       re.compile(r"{}\s*,\s*([\w]+)".format('Lot ID')),
      'date':         re.compile(r"{}\s*,\s*([0-9/]+)".format('Timestamp \(End\)'))}



for k, v in dd.items():
    print(k)
    match = dd[k].search(string)
    if match:
        print(match)
        res = match.group(1)
        print(res)
    else:
        res = match
        print(res)
    fin[k] = res

#%% KF
p = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GMTest\KA01017-FEB26-2021.TXT"

with open(p, 'r') as f:
    string = f.read()
    
    
fin ={'lot_id': None,
             'device_name': None,
             'process_name': None,
             'pcm_spec_name': None,
             'wafer_qty': None,
             'date': None
             }


dd = {'device_name':  re.compile(r"{}\s*\w*/([\w-]+)".format('DEVICE NAME')),
      'process_name': re.compile(r"{}\s*([\w]+)".format('PROCESS NAME')),
      'wafer_qty': re.compile(r"{}\s*([0-9]+)".format('WAFER/LOT QTY')),
      'date': re.compile(r"{}\s*([0-9/]+)".format('DATE')),
      'pcm_spec_name': re.compile(r"{}\s*([\w]+)".format('PCM SPEC NAME')),
      'lot_id': re.compile(r"{}\s*([\w]+)".format('LOT NO'))}



for k, v in dd.items():
    
    match = dd[k].search(string)
    if match:
        res = match.group(1)
        print(k, res)
    else:
        res = match
        print(k, res)
    fin[k] = res
# ll = ['TYPE NO', 'PROCESS', 'PCM SPEC']

# regexp = re.compile(r"{}\s*:\s*([\w\.-_!@]+)".format('LOT ID'))

# m = regexp.search(string)
# print(m.group(1))

# s = "hi my name is ryan, and i am new to python and would like to learn more"
# mm = re.search("^name: (\w+)", s)

# regexp = re.compile("name(.*)$")
# print(regexp.search(s).group(1))


# import re
# s = "hi my name is ryan, and i am new to python and would like to learn more"
# m = re.search("^name: (\w+)", s)

#%%
path = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GF\WAT\7knw52763.000.csv"
import numpy as np
def convert_to_numeric(df):
   df = df.applymap(lambda x: x.strip() if isinstance(x,str) else x)
   df = df.replace('', np.nan)
   df = df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
   return df

with open(path, 'r') as f:
    lines = f.readlines()


params_header = pd.DataFrame()
body_header = [['Wafer ID/Alias','param'], ['Unit', 'unit'],['SPEC HIGH', 'spec_high'], ['SPEC LOW','spec_low']]
body = []

for line in lines:
    line = line.strip()
    for bd in body_header:
        if bd[0] in line:
            print(bd)
            temp = line.split(',')
            temp.insert(0, bd[1])
            body.append(temp)

    

body_df = pd.DataFrame(body)
body_df = body_df.T
body_df = body_df.rename(columns=body_df.iloc[0]).drop(body_df.index[0])
body_df = convert_to_numeric(body_df)
body_df = body_df.dropna(how='any')
body_df = convert_to_numeric(body_df)

#%%
body_df = pd.DataFrame(body)
body_df = body_df.T
body_df = convert_to_numeric(body_df)
body_df = body_df.rename(columns=body_df.iloc[0]).drop(body_df.index[0])

body_df = body_df.dropna(how='any')
body_df = convert_to_numeric(body_df)
return body_df

params_header = pd.DataFrame()
body_header = ['Wafer ID/Alias', 'Unit','SPEC HIGH', 'SPEC LOW']
body = []

for line in self.lines:
    line = line.strip()
    if line.startswith(tuple(body_header)):
        temp = re.sub("\s\s+", "|", line)
        temp = temp.split('|')
        
        body.append(temp)

body = []

for line in self.lines:
    line = line.strip()

    if 'Wafer ID/Alias' in line or 'Unit' in line or 'SPEC HIGH' in line or 'SPEC LOW' in line:
        temp = line.split(',')
        body.append(temp)
    
body_df = pd.DataFrame(body)
if body_df.empty:
    return body_df

body_df = pd.DataFrame(body)
if body_df.empty:
    return body_df

body_df = body_df.dropna(how='all', axis=1)
body_df = body_df.rename(columns=body_df.iloc[0]).drop(body_df.index[0])

body_df.columns = [x.strip() for x in body_df.columns]

body_df = body_df.drop(columns = ['Site_X','Site_Y','Pass/Fail','Wafer ID/Alias','SiteID','Vendor Wafer Scribe ID'], axis=1)
body_df = body_df.T.reset_index()
body_df.columns = ['param', 'unit', 'spec_high', 'spec_low']
body_df = convert_to_numeric(body_df)
body_df = body_df.dropna(how='all')
# body_df.columns = [x.strip() for x in body_df.columns]
# body_df = body_df.drop(columns = ['Site_X','Site_Y','Pass/Fail','Wafer ID/Alias','SiteID','Vendor Wafer Scribe ID'], axis=1)
# body_df = convert_to_numeric(body_df)
# body_df = body_df.dropna(how='all', axis=1)
# body_df = body_df.T.reset_index()
# body_df.columns = ['param', 'unit', 'spec_high', 'spec_low']
# body_df = convert_to_numeric(body_df)
# body_df = body_df.dropna(how='all')
#%%
loo = '    '.strip()

print(len(loo))

#%%

temp = ''' 401-015-42L 401-015-42L sefgser sdrgserb :?!'''

prod = re.findall(r'\d{3}-\d{3}', temp)

print(prod)






