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
    
    def readfile(self, filepath):
        
        if os.path.exists(path) and os.stat(path) != 0:
            with open(filepath, 'r', encoding = 'utf8') as f:
                lines = f.readlines()
            return lines
        else:
            raise FileNotFoundError(f'{path} File does not exist or it is empty')
            
    @abstractmethod
    def initParams(self):
        pass
    @abstractmethod
    def parse_title(self):
        pass
    @abstractmethod
    def parse_body_header(self):
        pass
    @abstractmethod      
    def parse_body(self):
        pass
        
class TSMC(BaseParse):
    
    def __init__(self, filepath):
        self.filepath = filepath
        self.lines = self.dodo()
    
    def dodo(self):
        return self.readfile(self.filepath)
        
        
        
ff = TSMC(path)





#%%

def convert_to_numeric(df):
   df = df.applymap(lambda x: x.replace(' ','') if isinstance(x,str) else x)
   df = df.replace('', np.nan)
   df = df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
   return df




class ParsePcmTSMC:
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
        process_name
        pcm_spec_name
        wafer_count (quantaty of wafer in lot)
    
    Fields store in internal parameter obj.title_params 
    
    
    parse_params_header()
    
    Collects spec limits, parameter name, unit and store it in internal parameter obj.params_header
        
    
    """
    
    
    
    
    def __init__(self, filepath):
        self.filepath = filepath
        self._readfile()
        self._initParams()
        
    def _readfile(self):
        
        if os.path.exists(path) and os.stat(path) != 0:
            with open(self.filepath, 'r', encoding = 'utf8') as f:
                self.lines = f.readlines()
        else:
            raise FileNotFoundError(f'{path} File does not exist or it is empty')
            
    def _initParams(self):
        
        self.title_params = { 
                          'lot_id': None,
                          'type_no': None,
                          'process': None,
                          'pcm_spec': None,
                          'wafer_qty': None,
                          'date': None
                          }
        
        self.data_limits = None
        self.body = pd.DataFrame()
        self.title_df = None
        self.params_header = pd.DataFrame()
        self.error = False
        self.error_msg = []
        self.limits = None
        self.params = None
        
    def parse_title(self):

        for line in self.lines:
            
            try:
                line = line.strip()
                if line.startswith('TYPE NO'):
                    line = re.sub('   +', '||', line)
                    t_split = line.split('||')
                    
                    for t in t_split:
                        if t.startswith('TYPE NO'):
                            t = t.split(':')[1].strip()   
                            self.title_params['type_no'] = t
    
                        if t.startswith('PROCESS'):
                            t = t.split(':')[1].strip()
                            self.title_params['process'] = t

                        if t.startswith('PCM SPEC'):
                            t = t.split(':')[1].strip()
                            self.title_params['pcm_spec'] = t
   
                        if t.startswith('QTY'):
                            t = t.split(':')[1].strip()
                            t = t.split()[0]
                            self.title_params['wafer_qty'] = t

                    
                if line.startswith('LOT ID'):
                    lot_id = line.split('DATE')[0].strip().split(':')[1].strip().split('.')[0]
                    self.title_params['lot_id'] = lot_id
                    date = line.split('DATE')[1].strip().split(':')[1].strip()
                    date = datetime.strptime(date, '%m/%d/%Y')
                    self.title_params['date'] = date


            except Exception:
                    self._error_log()
                    
        return self.title_params
                
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
       
    def parse_params_header(self):
        
        try:
            
            sliced_data = self._slice_data()
            # clear data and grab limits and column names
            count = 0
            for b in sliced_data:
                
                header = []
                for x in b:
                    x = x.strip()
                    
                    if x.startswith('--'):
                        continue
                    
                    if x.startswith('ID') or x.startswith('SPEC') or x.startswith('WAF'):
                        count = count + 1
                        temp = re.sub(' +', '||', x)
                        temp = temp.split('||')
                        header.append(temp)
                        
                df = pd.DataFrame(header[1:], columns=header[0])
                df = df.drop(columns=['WAF', 'SITE'])
                df = df.T.reset_index()
                df.columns = ['param', 'unit', 'usl', 'lsl']
                self.params_header = pd.concat([self.params_header, df])
                
            self.params_header = convert_to_numeric(self.params_header)
                
            self.limits = self.params_header.set_index('param')         
            self.limits = self.limits.to_dict('index')     
            self.params = self.params_header['param'].tolist()
            return self.params_header
        
        except Exception as e:
            print(e)
            self._error_log()
            

      # 

        
    def parse_body(self):
        
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
                self.body = pd.concat([self.body, temp])
                
        except Exception:
            self._error_log()
        
        return self.body
            
    def add_title_to_data(self):
        
        # self.parse_title()
        for k,v in self.title_params.items():
            
            self.body.insert(0, k,v)
                    
        self.body = convert_to_numeric(self.body)
        self.body['date'] = pd.to_datetime(self.body['date'])
        
        
    def add_limits(self):
        
        try:

            self.body['spec_low'] = np.nan
            self.body['spec_high'] = np.nan
            self.body['unit'] = np.nan
            
            for param in self.params:
                spec_low = self.limits[param]['lsl']
                spec_high = self.limits[param]['usl']
                unit = self.limits[param]['unit']
                self.body.loc[(self.body['param'] == param), 'spec_low'] = spec_low
                self.body.loc[(self.body['param'] == param), 'spec_high'] = spec_high
                self.body.loc[(self.body['param'] == param), 'unit'] = unit
        
        except Exception as e:
            print(e)
            self._error_log()
      
    def title_to_dataframe(self):
        
        self.parse_title()
        self.title_df = pd.DataFrame([self.title_params])
        self.title_df = convert_to_numeric(self.title_df)
        self.title_df['date'] = pd.to_datetime(self.title_df['date'])
        return self.title_df
        
    def header_check(self):
        
        for k,v in self.title_params.items():
            if v is None or len(str(v)) == 0:
                self.error = True
                error_msg = f'{k} is missing'
                self.error_msg.append(error_msg)
        
    
    def get_date(self):
        self.parse_header()
        return self.title_params['date']
    
    def get_params_list(self):
        self.parse_params_header()
        return self.params
    
    def get_limits_in_dict(self):
        self.parse_params_header()
        return self.limits
    
    def get_product(self):
        self.parse_header()
        return self.title_params['device_name']
    
    def get_lotId(self):
        self.parse_header()
        return self.title_params['lot_id']
    
    def get_compelete_data(self):
        
        self.parse_title()
        self.parse_body()
        self.parse_params_header()
        self.add_header_to_data()
        self.add_limits()
        
        return self.body
         


path = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\TSMC\C00P57.00.txt"

lala = ParsePcmTSMC(path)
# lala.parse_title()

# data = lala.parse_params_header()

# foo = lala.params
# loo = lala.limits

# lala.parse_body()
# lala.add_limits()

# boo = lala.title_params


# lala.add_header_to_data()

# ddd = lala.body


dodo = lala.get_compelete_data()
# print(lala.error, lala.error_msg)

#%%

print(ParsePcmTSMC.__doc__)

#%%
path = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\TSMC\C00Plkj7.00.txt"

if os.path.exists(path) and os.stat(path) != 0:
    with open(path, 'r', encoding = 'utf8') as f:
        lines = f.readlines()
else:
    raise FileNotFoundError(f'{path} File does not exist or it is empty')

