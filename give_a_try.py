# -*- coding: utf-8 -*-
"""
Created on Mon Mar 27 22:14:34 2023

@author: vsviders
"""
import sys
sys.path.insert(0, r'C:\Users\VSviders\Documents\devs\E-test\ETEST_data\ParseModule')

import glob

import ParseModule

path1 = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\TSMC\C00P69.00.txt"
path2 = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GMTest\KA03483-MAR24-2021.TXT"
path3 = r"C:\Users\VSviders\Documents\devs\E-test\ETEST_data\GF\WAT\7knu40973.000.csv.csv"

for ind, path in enumerate(glob.glob(r'C:\Users\VSviders\Documents\devs\E-test\ETEST_data\TSMC\*')):
    
    try:
        print(ind)
        tsmc = ParseModule.TSMC(path)
        # tsmc_df = tsmc.get_compelete_df()
        # check1 = tsmc.body_header_check()
        # check2 = tsmc.title_check()
        check3 = tsmc.file_check()
    
        
    
    except Exception as e:
        print(e)
        raise
    
# dfd = tsmc.parse_body_header()
    # kf = ParseModule.KeyFoundry(path2)
    # gf = ParseModule.GF(path3)


print('lolo')
    



# print(tsmc.error, tsmc.error_msg)
# kf_df = kf.get_compelete_df()
# gf_df = gf.get_compelete_df()

#%%

import pandas as pd
import numpy as np
import re
from datetime import datetime
import sys
import traceback
import os
from abc import ABC, abstractmethod

import functools

error = False
error_msgs = []

def _error_log():
    error = True
    exc_type, exc_value, exc_traceback = sys.exc_info() 
    traceback_details = {
                         'filename': exc_traceback.tb_frame.f_code.co_filename,
                         'lineno'  : exc_traceback.tb_lineno,
                         'name'    : exc_traceback.tb_frame.f_code.co_name,
                         'type'    : exc_type.__name__,
                         'message' : traceback.format_exc()
                        }
    error_msg = f"Function {traceback_details['name']}, Line {traceback_details['lineno']}, type {traceback_details['type']}"
    error_msgs.append(error_msg)

def exception_handler(func):

    # @functools.wraps(func)
    def inner_function(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            print(e)
            exc_type, exc_value, exc_traceback = sys.exc_info() 
            traceback_details = {
                                 'filename': exc_traceback.tb_frame.f_code.co_filename,
                                 'lineno'  : exc_traceback.tb_lineno,
                                 'name'    : exc_traceback.tb_frame.f_code.co_name,
                                 'type'    : exc_type.__name__,
                                 'message' : traceback.format_exc()
                                }
            error_msg = f"Function {func.__name__}, Line {traceback_details['lineno']}, type {traceback_details['type']} "
            print(error_msg, )    
            error_msgs.append(error_msg)
    return inner_function


@exception_handler
def area_square(length):
    print(length * length)
    raise TypeError('Some thing wrong with this operation')
    
    
area_square(5)

