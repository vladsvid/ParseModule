# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 09:33:54 2023

@author: vsviders
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime
import sys
import traceback
import os
from abc import ABC, abstractmethod

path = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\TSMC\C00P57.00.txt"

class BaseParse(ABC):
    
    def get_title_params_init(self):
        return  {    'lot_id': None,
                    'device_name': None,
                    'process_name': None,
                    'pcm_spec_name': None,
                    'wafer_qty': None,
                    'date': None
                    }
    
    def readfile(self, filepath):
        if os.path.exists(path) and os.stat(path) != 0:
            with open(filepath, 'r', encoding = 'utf8') as f:
                lines = f.readlines()
            return lines
        else:
            raise FileNotFoundError(f'{path} File does not exist or it is empty')
            

    @abstractmethod
    def parse_title(self):
        pass
    @abstractmethod
    def parse_body_header(self):
        pass
    @abstractmethod      
    def parse_body(self):
        pass
    
    def _error_log(self):
        self.error = True
        exc_type, exc_value, exc_traceback = sys.exc_info() 
        traceback_details = {
                             'filename': exc_traceback.tb_frame.f_code.co_filename,
                             'lineno'  : exc_traceback.tb_lineno,
                             'name'    : exc_traceback.tb_frame.f_code.co_name,
                             'type'    : exc_type.__name__,
                             'message' : traceback.format_exc()
                            }
        error_msg = f"Function {traceback_details['name']}, Line {traceback_details['lineno']}, type {traceback_details['type']}"
        self.error_msg.append(error_msg)
        
    def add_title_to_data(self, df, title_params):     
        for k,v in title_params.items():
            
            if v is None:
                df.insert(0, k,'Null')
            else:
                df.insert(0, k,v)
                    
        df = convert_to_numeric(df)
        df['date'] = pd.to_datetime(df['date'])
        return df

    def add_limits(self, df, params_header):
        try:
            limits = params_header.set_index('param')         
            limits = limits.to_dict('index')     
            params = params_header['param'].tolist()

            df['spec_low'] = np.nan
            df['spec_high'] = np.nan
            df['unit'] = np.nan
            
            for param in params:
                spec_low = limits[param]['spec_low']
                spec_high = limits[param]['spec_high']
                unit = limits[param]['unit']
                df.loc[(df['param'] == param), 'spec_low'] = spec_low
                df.loc[(df['param'] == param), 'spec_high'] = spec_high
                df.loc[(df['param'] == param), 'unit'] = unit
            return df
        
        except Exception as e:
            print(e)
            self._error_log()
            
    def get_date(self):
        title_params = self.parse_title()
        return title_params['date']
        
    def get_product(self):
        title_params = self.parse_title()
        return title_params['device_name']

    def get_lotId(self):
        title_params = self.parse_title()
        return title_params['lot_id']

    def title_to_dataframe(self):
        title_params = self.parse_title()
        title_df = pd.DataFrame([title_params])
        title_df = convert_to_numeric(title_df)
        title_df['date'] = pd.to_datetime(title_df['date'])
        return title_df
    
    def get_compelete_df(self):
        title_params = self.parse_title()
        df = self.parse_body()
        params_header = self.parse_body_header()
        self.add_title_to_data(df, title_params)
        self.add_limits(df, params_header)
        
        return df
    
def convert_to_numeric(df):
   df = df.applymap(lambda x: x.replace(' ','') if isinstance(x,str) else x)
   df = df.replace('', np.nan)
   df = df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
   return df

class TSMC(BaseParse):
    """
    
    A class used to parse E-test data files from TSMC foundry
    
    Attributes:
    -----------
    filepath : str
        absolute file path to e-test file
    
    Methods:
    -------
    
    parse_title()
    
    Parse the title of file and retrieve fileds like:
        lot_id
        device_name
        wafer_test_date
        process_name_name
        pcm_spec_name
        wafer_count (quantaty of wafer in lot)
    
    Fields store in internal parameter obj.title_params 
    
    
    parse_params_header()
    
    Collects spec limits, parameter name, unit and store it in internal parameter obj.params_header
        
    
    """
    
    def __init__(self, filepath):
        self.filepath = filepath
        self.lines = self.readfile(self.filepath)

        self.error = False
        self.error_msg = []
        
        
    def parse_title(self):
        
        title_params = self.get_title_params_init()

        try:
            for line in self.lines:
                
      
                    line = line.strip()
                    if line.startswith('TYPE NO'):
                        line = re.sub('   +', '||', line)
                        t_split = line.split('||')
                        
                        for t in t_split:
                            if t.startswith('TYPE NO'):
                                t = t.split(':')[1].strip()   
                                title_params['device_name'] = t
        
                            if t.startswith('PROCESS'):
                                t = t.split(':')[1].strip()
                                title_params['process_name'] = t
    
                            if t.startswith('PCM SPEC'):
                                t = t.split(':')[1].strip()
                                title_params['pcm_spec_name'] = t
       
                            if t.startswith('QTY'):
                                t = t.split(':')[1].strip()
                                t = t.split()[0]
                                title_params['wafer_qty'] = t
    
                        
                    if line.startswith('LOT ID'):
                        lot_id = line.split('DATE')[0].strip().split(':')[1].strip().split('.')[0]
                        title_params['lot_id'] = lot_id
                        date = line.split('DATE')[1].strip().split(':')[1].strip()
                        date = datetime.strptime(date, '%m/%d/%Y')
                        title_params['date'] = date
                        
            return title_params


        except Exception:
            self._error_log()
                    

              
    def _slice_data(self):
        indeces = []
        body_data = []
        
        for line in self.lines:
            if line.startswith(' WAF'):
                ind = self.lines.index(line)
                indeces.append(ind)
    
        for n in range(len(indeces)-1):
            first = indeces[n]
            second = indeces[n+1]
            temp = self.lines[first:second]
            body_data.append(temp)
            
        return body_data
    
       
    def parse_body_header(self):
        params_header = pd.DataFrame()
        try:
            
            sliced_data = self._slice_data()
            # clear data and grab limits and column names
            for b in sliced_data:
                
                header = []
                for x in b:
                    x = x.strip()
                    
                    if x.startswith('--'):
                        continue
                    
                    if x.startswith('ID') or x.startswith('SPEC') or x.startswith('WAF'):
                        temp = re.sub(' +', '||', x)
                        temp = temp.split('||')
                        header.append(temp)
                        
                df = pd.DataFrame(header[1:], columns=header[0])
                df = df.drop(columns=['WAF', 'SITE'])
                df = df.T.reset_index()
                df.columns = ['param', 'unit', 'spec_high', 'spec_low']
                params_header = pd.concat([params_header, df])
                
            params_header = convert_to_numeric(params_header)
                

            return params_header
        
        except Exception as e:
            print(e)
            self._error_log()
            


    def parse_body(self):
        
        bb = pd.DataFrame()
        
        try:      
            sliced_data = self._slice_data()

            for b in sliced_data:
                body = []
                for x in b:
                    x = x.strip()
                    
                    if x.startswith('--'):
                        continue
                    
                    if x.startswith('WAF'):
                        temp = re.sub('  +', ' ', x)
                        temp = temp.split()
                        body.append(temp)
                        
                    if x[:2].strip().isdigit():
                        body.append(x.split())
                        
                    

                body_df = pd.DataFrame(body[1:], columns=body[0])
                body_df.columns = [x.strip() for x in body_df.columns]
                body_df = convert_to_numeric(body_df)

                temp = body_df.set_index(['WAF', 'SITE']).stack().reset_index()
                temp.columns = ['wafer_id', 'structure_loc','param', 'value']
                bb = pd.concat([bb, temp])
            
            return bb
                
        except Exception:
            self._error_log()


path = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\TSMC\C00P57.00.txt"

ff = TSMC(path)

df = ff.get_compelete_df()

date = ff.get_date()

lot = ff.get_lotId()

print(date, lot)

print(ff.error, ff.error_msg)

#%%
class KeyFoundry(BaseParse):
    
        
    def __init__(self, filepath):
        self.filepath = filepath
        self.lines = self.readfile(self.filepath)
        self.error = False
        self.error_msg = []
        
    def parse_title(self):
        title_params = self.get_title_params_init()
        try:
            for line in self.lines:
                
    
                    if line.startswith('LOT NO'):      
                        lot_id = line.replace('LOT NO', '').strip()
                        title_params['lot_id'] = lot_id
                        
                    if line.startswith('DEVICE NAME'):         
                        temp = line.replace('DEVICE NAME', '').strip().split('/')[1]
                        title_params['device_name'] = temp
                        
                        
                    if line.startswith('TEST PATTERN NAME'):            
                        temp1 = line.split(',')[1].replace('DATE', '').strip()
                        temp1 = datetime.strptime(temp1, '%Y/%m/%d %H:%M:%S')
                        title_params['date'] = temp1
    
                        
                    if line.startswith('PCM SPEC NAME'):           
                        temp1 = line.split(',')[1].replace('WAFER/LOT QTY', '').strip()
                        temp2 = line.split(',')[0].replace('PCM SPEC NAME', '').strip()
                        title_params['wafer_qty'] = temp1
                        title_params['pcm_spec_name'] = temp2
                        
                    if line.startswith('PROCESS NAME'):           
                        temp1 = line.split(',')[0].replace('PROCESS NAME', '').strip()
                        title_params['process_name'] = temp1
            return title_params
        
        except Exception:
            self._error_log()
       
    def parse_body_header(self):
        
        body_header = ['PARAMETER', 'SPEC', 'WAFER']
        body = []
        
        try:
            
            for line in self.lines:
                
                if line.startswith(tuple(body_header)):
                    temp = re.sub("\s\s+", "|", line)
                    temp = temp.split('|')
                    
                    body.append(temp)
                    
    
            df = pd.DataFrame(body).T
            df = df.rename(columns=df.iloc[0]).drop(df.index[0])
            df = df.rename(columns ={'WAFER ID / UNIT' : 'unit', 'SPEC LOW': 'spec_low', 'SPEC HIGH': 'spec_high', 'PARAMETER': 'param'})
    
            df = convert_to_numeric(df)
            df = df.dropna()
        

            return df
    
        except Exception:
            self._error_log()
                
    def parse_body(self):
        
        body = []
        try:
            for line in self.lines:
                line = line.strip()
    
                
                if line.startswith('PARAMETER'):
                    temp = re.sub('\s\s+', '|', line)
                    temp = temp.split('|')
                    body.append(temp)
                    
                if line[:2].strip().isdigit():
                    temp = re.sub('\s\s+', '|', line)
                    body.append(temp.split('|'))
                    
                
    
            body_df = pd.DataFrame(body[1:], columns=body[0])
            body_df.columns = [x.strip() for x in body_df.columns]
            
            
            wafer_id =  body_df['PARAMETER'].apply(lambda x: x.split(' ')[0])
            structure_location =  body_df['PARAMETER'].apply(lambda x: x.split(' ')[1])
            
            body_df = body_df.drop('PARAMETER', axis=1)
            body_df.insert(0,'wafer_id', wafer_id)
            body_df.insert(0,'structure_location', structure_location)
            
            body_df = body_df.set_index(['wafer_id', 'structure_location']).stack().reset_index()
            body_df.columns = ['wafer_id', 'structure_loc','param', 'value']
            
            body_df = convert_to_numeric(body_df)

            
            return body_df
        
        except Exception:
            self._error_log()
            
         
file = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GMTest\KA03483-MAR24-2021.TXT"

lala = KeyFoundry(file)

df = lala.parse_body_header()

ddf = lala.get_compelete_df()

#%%

class GF(BaseParse):
    
    def __init__(self, filepath):
        self.filepath = filepath
        self.lines = self.readfile(self.filepath)
        self.error = False
        self.error_msg = []
        
    def parse_title(self):
        title_params = self.get_title_params_init()
        try:
            for line in self.lines:
                
                    if line.startswith('Lot ID'):      
                        lot_id = line.split(',')[1].split('.')[0].strip()
                        title_params['lot_id'] = lot_id
                        
                    if line.startswith('Product'):         
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
    
            return title_params
        
        except Exception:
            self._error_log()
            
    
    def parse_body(self):
        
        body = []
        try:
            for line in self.lines:
                
                line = line.strip()
                if line.startswith('WaferID'):
                    temp = line.split(',')
                    body.append(temp)
                    
                if line[:2].strip().isdigit() or line[:1].strip().isdigit():
                    temp = line.split(',')
                    body.append(temp)
            
            body_df = pd.DataFrame(body[1:], columns=body[0])
            body_df.columns = [x.strip() for x in body_df.columns]
            
            body_df = body_df.drop(columns = ['Site_X','Site_Y', 'Pass/Fail'], axis=1)
            body_df = convert_to_numeric(body_df)
            body_df = body_df.dropna(how='all', axis=1)
            
            body_df = body_df.set_index(['WaferID', 'SiteID']).stack().reset_index()
            body_df.columns = ['wafer_id', 'structure_loc','param', 'value']
            body_df = convert_to_numeric(body_df)
            
            return body_df

        except Exception:
            self._error_log()
    
    def parse_body_header(self):
        body = []
        
        try:
            

            for line in self.lines:
                line = line.strip()
    
                if 'WaferID' in line or 'Unit' in line or 'SPEC HIGH' in line or 'SPEC LOW' in line:
                    temp = line.split(',')
                    body.append(temp)
                
            body_df = pd.DataFrame(body)
            body_df = convert_to_numeric(body_df)
            body_df = body_df.dropna(how='all', axis=1)
            body_df = body_df.rename(columns=body_df.iloc[0]).drop(body_df.index[0])
            body_df.columns = [x.strip() for x in body_df.columns]
            body_df = body_df.drop(columns = ['Site_X','Site_Y', 'Pass/Fail', 'WaferID', 'SiteID'], axis=1)
            body_df = body_df.T.reset_index()
            body_df.columns = ['param', 'unit', 'spec_high', 'spec_low']
            body_df = convert_to_numeric(body_df)
            
            return body_df
        
        except Exception:
            self._error_log()
            

file = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GF\WAT\7knu40973.000.csv.csv"

lala = GF(file)

gf_title = lala.parse_title()
gf_body = lala.parse_body()
gf_body_header = lala.parse_body_header()

gf_df = lala.add_title_to_data(gf_body, gf_title)

df_ddf = lala.add_limits(gf_df, gf_body_header)


ddd = lala.get_date()

lot = lala.get_lotId()

print(ddd, lot)

#%%

print(ParsePcmTSMC.__doc__)

#%%
path = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\TSMC\C00Plkj7.00.txt"

if os.path.exists(path) and os.stat(path) != 0:
    with open(path, 'r', encoding = 'utf8') as f:
        lines = f.readlines()
else:
    raise FileNotFoundError(f'{path} File does not exist or it is empty')

#%%

import pandas as pd

file = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GF\WAT\7knu40973.000.csv.csv"

with open(file, 'r') as f:
    lines = f.readlines()

body = []

for line in lines:
    line = line.strip()

    if 'WaferID' in line or 'Unit' in line or 'SPEC HIGH' in line or 'SPEC LOW' in line:
        temp = line.split(',')
        body.append(temp)
    
body_df = pd.DataFrame(body)
body_df = convert_to_numeric(body_df)
body_df = body_df.dropna(how='all', axis=1)
body_df = body_df.rename(columns=body_df.iloc[0]).drop(body_df.index[0])
body_df.columns = [x.strip() for x in body_df.columns]
body_df = body_df.drop(columns = ['Site_X','Site_Y', 'Pass/Fail', 'WaferID', 'SiteID'], axis=1)
body_df = body_df.T.reset_index()
body_df.columns = ['param', 'unit', 'spec_high', 'spec_low']
body_df = convert_to_numeric(body_df)

#%%
title_params_init = { 
                  'lot_id': 34,
                  'device_name': 45,
                  'process_name': 32,
                  'pcm_spec_name': None,
                  'wafer_qty': 564,
                  'date': 544
                  }



for k,v in title_params_init.items():
    
    if v is None:
        body_df.insert(0, k,'Null')
        print('we are here')
    else:
        body_df.insert(0, k,v)



#%%


body_df = convert_to_numeric(body_df)
body_df = body_df.dropna(how='all', axis=1)


#%%
file = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GMTest\KA03483-MAR24-2021.TXT"
import re

def convert_to_numeric(df):
   df = df.applymap(lambda x: x.replace(' ','') if isinstance(x,str) else x)
   df = df.replace('', np.nan)
   df = df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
   return df

with open(file, 'r') as f:
    lines = f.readlines()

body_header = ['PARAMETER', 'SPEC', 'WAFER']

ll = []
for line in lines:
    
    if line.startswith(tuple(body_header)):
        temp = re.sub("\s\s+", "|", line)
        temp = temp.split('|')
        
        ll.append(temp)
        

df = pd.DataFrame(ll).T
df = df.rename(columns=df.iloc[0]).drop(df.index[0])

df = convert_to_numeric(df)
df = df.dropna()


#%%
path = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GMTest\KA03928-APR21-2021.TXT"

with open(file, 'r') as f:
    lines = f.readlines()
    

body_header = ['PARAMETER', 'SPEC', 'WAFER']
dodo = []


    
for line in lines:
    
    if line.startswith(tuple(body_header)):
        temp = re.sub("\s\s+", "|", line)
        temp = temp.split('|')
        
        dodo.append(temp)
        

df = pd.DataFrame(dodo).T
df = df.rename(columns=df.iloc[0]).drop(df.index[0])
df = df.rename(columns ={'WAFER ID / UNIT' : 'unit', 'SPEC LOW': 'spec_low', 'SPEC HIGH': 'spec_high'})

df = convert_to_numeric(df)
df = df.dropna()




#%%

body = []

for line in lines:
    line = line.strip()

    
    if line.startswith('PARAMETER'):
        temp = re.sub('\s\s+', '|', line)
        temp = temp.split('|')
        body.append(temp)
        
    if line[:2].strip().isdigit():
        temp = re.sub('\s\s+', '|', line)
        body.append(temp.split('|'))
        
    

body_df = pd.DataFrame(body[1:], columns=body[0])
body_df.columns = [x.strip() for x in body_df.columns]


wafer_id =  body_df['PARAMETER'].apply(lambda x: x.split(' ')[0])
structure_location =  body_df['PARAMETER'].apply(lambda x: x.split(' ')[1])

body_df = body_df.drop('PARAMETER', axis=1)
body_df.insert(0,'wafer_id', wafer_id)
body_df.insert(0,'structure_location', structure_location)
body_df = convert_to_numeric(body_df)

body_df = body_df.set_index(['wafer_id', 'structure_location']).stack().reset_index()
body_df.columns = ['wafer_id', 'structure_loc','param', 'value']





