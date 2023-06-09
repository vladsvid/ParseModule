# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 09:33:54 2023

@author: vsviders
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime
import os
from abc import ABC, abstractmethod



def convert_to_numeric(df):
   df = df.applymap(lambda x: x.strip() if isinstance(x,str) else x)
   df = df.replace('', np.nan)
   df = df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
   return df

class BaseParse(ABC):
    
    def get_title_params_init(self, foundry_name):
       params_init ={'lot_id': '',
                    'device_name': '',
                    'process_name': '',
                    'pcm_spec_name': '',
                    'wafer_qty': '',
                    'date': '',
                    'ic_universal_part_no': ''
                    }
       params_init['foundry_name'] = foundry_name
        
       return  params_init

    def readfile(self, filepath):
        if os.path.isfile(filepath) and os.stat(filepath).st_size != 0:
            with open(filepath, 'r', encoding = 'utf8') as f:
                lines = f.readlines()
            return lines
        else:
            raise FileNotFoundError(f'{filepath} File does not exist or it is empty')
            
    @abstractmethod
    def parse_title(self):
        pass
    @abstractmethod
    def parse_body_header(self):
        pass
    @abstractmethod  
    def parse_body(self):
        pass
           
    def add_title_to_data(self, df, title_params):     
        for k,v in title_params.items():
            df.insert(0, k,v)
            
        df = convert_to_numeric(df)
        df['date'] = pd.to_datetime(df['date'])
        df = df.drop(columns=['wafer_qty'], axis=1)
        return df
    
    def add_limits(self, df, params_header):

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
    
    def add_ic_universal(self, title_params):
        
        dev_name = title_params.get('device_name')
        prod = re.findall(r'\d{3}-\d{3}', dev_name)
        if prod:
            title_params['ic_universal_part_no'] = prod[0]
        else:
            title_params['ic_universal_part_no'] = dev_name
            
        return title_params
    
    def title_to_dataframe(self):
        title_params = self.parse_title()
        title_df = pd.DataFrame([title_params])
        title_df = convert_to_numeric(title_df)
        title_df['date'] = pd.to_datetime(title_df['date'])
        return title_df
        
    def get_date(self):
        title_params = self.parse_title()
        return title_params['date']
       
    def get_product(self):
        title_params = self.parse_title()
        return title_params['device_name']
    
    def get_lotId(self):
        title_params = self.parse_title()
        return title_params['lot_id']
    
    def get_compelete_df(self, check=True):
        self.file_check()
        title_params = self.parse_title()
        title_params = self.add_ic_universal(title_params)
        
        df = self.parse_body()
        params_header = self.parse_body_header()
        df = self.add_title_to_data(df, title_params)
        df = self.add_limits(df, params_header)
        
        return df
    
    def body_header_check(self):
        check = self.parse_body_header()
        if check.empty:
            raise Exception('Header body is empty, check it out!')
            
    def body_check(self):
        check = self.parse_body()
        if check.empty:
            raise Exception('Body is empty, check it out!')
            
    def title_check(self):
        params = self.parse_title()
        params_list_mandatory = ['lot_id', 'device_name', 'date' ]
        
        ll = []
        
        for k , v in params.items():
            if k in params_list_mandatory:
                ll.append(v)
            
        if '' in ll:
            raise Exception('Mandatory title parameters are missing, check it out')

    def file_check(self):
        self.title_check()
        self.body_header_check()
        self.body_check()

class TSMC(BaseParse):

    def __init__(self, filepath):
        self.filepath = filepath
        self.lines = self.readfile(self.filepath)
        self.foundry_name = 'TSMC'

    def parse_title(self):
        title_params = self.get_title_params_init('TSMC')
        
        with open(self.filepath, 'r', encoding = 'utf8') as f:
            string = f.read()
    
        patterns = {'device_name':      re.compile(r"{}\s*:([\w-]+)".format('TYPE NO')),
                    'process_name':     re.compile(r"{}\s*:([\w-]+)".format('PROCESS')),
                    'wafer_qty':        re.compile(r"{}\s*:([0-9]+)".format('QTY')),
                    'date':             re.compile(r"{}\s*:([0-9/]+)".format('DATE')),
                    'pcm_spec_name':    re.compile(r"{}\s*:([\w-]+)".format('PCM SPEC')),
                    'lot_id':           re.compile(r"{}\s*:([\w]+)".format('LOT ID'))}
    
        for k, v in patterns.items():
            match = patterns[k].search(string)
            res = ''
            if match:
                res = match.group(1)
            title_params[k] = res
            
        return title_params

    def _slice_data(self):
        indeces = []
        body_data = []
        
        for line in self.lines:
            if line.startswith(' WAF'):
                ind = self.lines.index(line)
                indeces.append(ind)
        
        if len(indeces) == 1:
            temp = self.lines[indeces[0]:]
            body_data.append(temp)
            return body_data
    
        for n in range(len(indeces)-1):
            first = indeces[n]
            second = indeces[n+1]
            temp = self.lines[first:second]
            body_data.append(temp)
            
        return body_data
    
    def parse_body_header(self):
        params_header = pd.DataFrame()
        body_header = [['ID','unit'], ['SPEC HI', 'spec_high'], ['SPEC LO','spec_low'], ['WAF','param']]
        drop_list = ['WAF', 'SITE']
        
        sliced_data = self._slice_data()
        
        if len(sliced_data) == 0:
            return params_header

        for b in sliced_data:
            body = []
            for line in b:
                
                line = line.strip()
                for bd in body_header:
                    if line.startswith(bd[0]):
                       
                        line = line.replace('SPEC HI', 'SPEC_HI  NULL')
                        line = line.replace('SPEC LO', 'SPEC_LO  NULL')
                        temp = re.sub('\s+', '|', line)
                        temp = temp.split('|')
                        temp.insert(0, bd[1])
                        body.append(temp)
                        
            if len(body) != len(body_header):
                return params_header
        
            temp_df = pd.DataFrame(body).T
            temp_df = temp_df.rename(columns=temp_df.iloc[0,:]).drop(temp_df.index[0])
            temp_df = temp_df[temp_df['param'].isin(drop_list) == False]
            params_header = pd.concat([params_header, temp_df])
            
        params_header = convert_to_numeric(params_header)
        
        return params_header
    
    def parse_body(self):
        body_df = pd.DataFrame()
        sliced_data = self._slice_data()
        
        if len(sliced_data) == 0:
            return body_df
        
        for b in sliced_data:
            body = []
            for x in b:
                x = x.strip()
                
                if x.startswith('WAF'):
                    temp = re.sub('\s+', '|', x)
                    temp = temp.split('|')
                    body.append(temp)
                    
                if x[:2].strip().isdigit() or x[:1].strip().isdigit():
                    body.append(x.split())
                    
            temp_df = pd.DataFrame(body)
            temp_df = temp_df.rename(columns=temp_df.iloc[0]).drop(temp_df.index[0])
            temp_df.columns = [x.strip() for x in temp_df.columns]
            temp_df = convert_to_numeric(temp_df)

            temp = temp_df.set_index(['WAF', 'SITE']).stack().reset_index()
            temp.columns = ['wafer_id', 'structure_loc','param', 'value']
            body_df = pd.concat([body_df, temp])
        
        return body_df
                
class KeyFoundry(BaseParse):
     
    def __init__(self, filepath):
        self.filepath = filepath
        self.lines = self.readfile(self.filepath)
     
    def parse_title(self):
        title_params = self.get_title_params_init('KF')
      
        with open(self.filepath, 'r', encoding = 'utf8') as f:
            string = f.read()
    
        patterns = {'device_name':      re.compile(r"{}\s*\w*/([\w-]+)".format('DEVICE NAME')),
                    'process_name':     re.compile(r"{}\s*([\w]+)".format('PROCESS NAME')),
                    'wafer_qty':        re.compile(r"{}\s*([0-9]+)".format('WAFER/LOT QTY')),
                    'date':             re.compile(r"{}\s*([0-9/]+)".format('DATE')),
                    'pcm_spec_name':    re.compile(r"{}\s*([\w]+)".format('PCM SPEC NAME')),
                    'lot_id':           re.compile(r"{}\s*([\w]+)".format('LOT NO'))}
    
        for k, v in patterns.items():
            match = patterns[k].search(string)
            res = ''
            if match:
                res = match.group(1)
            title_params[k] = res
            
        title_params = self.add_ic_universal(title_params)
            
        return title_params
        

       
    def parse_body_header(self):
        params_header = pd.DataFrame()
        body_header = [['PARAMETER', 'param'], ['SPEC LOW', 'spec_low'],['SPEC HIGH', 'spec_high'], ['WAFER ID / UNIT', 'unit']]
        drop_list = ['PARAMETER']
        body = []
        
        for line in self.lines:
            line = line.strip()
            for bd in body_header:
                if line.startswith(bd[0]):
                    temp = re.sub("\s\s+", "|", line)
                    temp = temp.split('|')
                    temp.insert(0, bd[1])
                    body.append(temp)


        if len(body_header) != len(body):
            return params_header
        
        params_header = pd.DataFrame(body).T
        params_header = params_header.rename(columns=params_header.iloc[0]).drop(params_header.index[0])
        params_header = params_header[params_header['param'].isin(drop_list) == False]
        params_header = convert_to_numeric(params_header)
        
        
    
        return params_header
    
    def parse_body(self):
        body = []
        
        for line in self.lines:
            line = line.strip()

            
            if line.startswith('PARAMETER'):
                temp = re.sub('\s\s+', '|', line)
                temp = temp.split('|')
                body.append(temp)
                
            if line[:2].strip().isdigit() or line[:1].strip().isdigit():
                temp = re.sub('\s\s+', '|', line)
                body.append(temp.split('|'))
                
            
        body_df = pd.DataFrame(body)
        body_df = body_df.rename(columns=body_df.iloc[0]).drop(body_df.index[0])

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

class GF(BaseParse):
    
    def __init__(self, filepath):
        self.filepath = filepath
        self.lines = self.readfile(self.filepath)

        
    def parse_title(self):
        title_params = self.get_title_params_init('GF')
   
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

            
    def parse_body(self):
        
        body = []

        for line in self.lines:
            
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
        body_df = body_df.dropna(how='all')
        return body_df
    
    
class GF2(BaseParse):
        
    def __init__(self, filepath):
        self.filepath = filepath
        self.lines = self.readfile(self.filepath)

    def parse_title(self):
        
        title_params = self.get_title_params_init('GF')
        
        with open(self.filepath, 'r', encoding = 'utf8') as f:
            string = f.read()
        
        patterns = {'device_name':  re.compile(r"{}\s*,([\w-]+)".format('Customer Product Name')),
                    'process_name': re.compile(r"{}\s*,([\w]+)".format('Technology')),
                    'wafer_qty':    re.compile(r"{}\s*,([0-9]+)".format('Wafer Count')),
                    'lot_id':       re.compile(r"{}\s*,([\w]+)".format('Lot ID')),
                    'date':         re.compile(r"{}\s*,([0-9/]+)".format('Timestamp \(End\)'))}
        
        for k, v in patterns.items():
            match = patterns[k].search(string)
            res = ''
            if match:
                res = match.group(1)
            title_params[k] = res
            
        return title_params
   
    def parse_body_header(self):
        
        params_header = pd.DataFrame()
        body_header = [['Wafer ID/Alias','param'], ['Unit', 'unit'],['SPEC HIGH', 'spec_high'], ['SPEC LOW','spec_low']]
        body = []

        for line in self.lines:
            line = line.strip()
            for bd in body_header:
                if bd[0] in line:
               
                    temp = line.split(',')
                    temp.insert(0, bd[1])
                    body.append(temp)

        if len(body_header) != len(body):
            return params_header
        
        params_header = pd.DataFrame(body)
        params_header = params_header.T
        params_header = params_header.rename(columns=params_header.iloc[0]).drop(params_header.index[0])
        params_header = convert_to_numeric(params_header)
        params_header = params_header.dropna(how='any')
        params_header = convert_to_numeric(params_header)
        return params_header
    
    def parse_body(self):

        body_df = pd.DataFrame()
        for line in self.lines:
            
            if line.startswith('Vendor Wafer Scribe ID'):
                line_ind = self.lines.index(line)
                body_df = pd.read_csv(self.filepath, skiprows = line_ind)
                break
        
        if body_df.empty:
            return body_df
        

    
        body_df = body_df[body_df['SiteID'].notna()]
        body_df = body_df.dropna(how='all', axis=1)

        body_df.columns = [x.strip() for x in body_df.columns]
        body_df = body_df.drop(columns = ['Site_X','Site_Y', 'Pass/Fail', 'Vendor Wafer Scribe ID'], axis=1)
        body_df = body_df.drop(body_df.filter(regex='Site_').columns, axis=1)
    
        body_df = body_df.set_index(['Wafer ID/Alias', 'SiteID']).stack().reset_index()
        body_df.columns = ['wafer_id', 'structure_loc','param', 'value']
        body_df = convert_to_numeric(body_df)
            
        return body_df
    

        
    
        



