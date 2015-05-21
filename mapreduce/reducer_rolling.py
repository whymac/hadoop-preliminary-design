#!/usr/bin/python
import sys
import pandas as pd
import numpy as np

def initialize():
    global num_observations
    global corr_df, betas_df
    global corr_dict, betas_dict
    global current_key, columns, market_list
    # set the number of obervations
    num_observations = 6
    #initialization
    current_key = None
    corr_dict = {}
    betas_dict = {}
    # use hash function on market obeservables if needed
    market_list = [hash_function('S&P500'), hash_function('oil price')]
    try:
        # get records from previous results
        columns = pd.read_pickle('return_dataframe').columns
        betas_df = pd.DataFrame([])
        corr_df = pd.DataFrame(1, index = columns, columns = columns)
    except IOError:
        corr_df = pd.DataFrame([])

def hash_function(x):
    if x == 'S&P500':
        return 'ff8b4404b8e217a65a81f351a7c41ef216ac160df0d61a8d1e7d94509f3e58d8'
    elif x == 'oil price':
        return 'fe5a263c3f5458f7f12cf159eaa98ead36b21ef7868b47f95ac0164c259f77fb'
    else:
        return x

def cal_corr(x,y,xx,yy,xy,num):
    numer = xy*num - x*y
    denom = np.sqrt((xx*num - x**2) * (yy*num - y**2))
    if denom == 0:
        return 0.0
    else:
        return numer/denom

def cal_beta(x,y,xx,xy,num):
    numer = xy*num - x*y
    denom = xx*num - x**2
    if denom == 0:
        return 0.0
    else:
        return numer/denom

def parseInput():
    for line in sys.stdin:
        yield line.strip()

def reducer():
    global num_observations
    global corr_df, betas_df, empty_df
    global corr_dict, betas_dict
    global current_key, columns, market_list
    # initialization
    initialize()
    for line in parseInput():
        temp_key, value = line.strip().split('\t')
        temp_key1, temp_key2, temp_key3 = temp_key.split(',')
        key = (temp_key1,temp_key2)
        print key
        value1, value2 = value.split(',')
        try:
            line_index = int(temp_key3)
            value1 = float(value1)
            value2 = float(value2)
        except ValueError:
            continue
        # calculate the first correlations matrix and betas using the previous methods
        if key == current_key:
            if line_index < num_observations:
                x_list.append(value1)
                y_list.append(value2)
                x += value1
                y += value2
                xx += value1*value1
                yy += value2*value2
                xy += value1*value2
                num += 1
                key3 = line_index
            # begins to process rolling observations
            else:
                corr = cal_corr(x,y,xx,yy,xy,num)
                corr_tempdf = corr_dict.get(key3, corr_df)
                corr_tempdf.ix[key1,key2] = corr
                corr_tempdf.ix[key2,key1] = corr
                corr_dict[key3] = corr_tempdf
                # see if the symbol
                if columns[key1] not in market_list and columns[key2] in market_list:
                    beta = cal_beta(y,x,yy,xy,num)
                    betas_tempdf = betas_dict.get(key3, betas_df)
                    betas_tempdf.ix[columns[key1],columns[key2]] = beta
                    betas_dict[key3] = betas_tempdf
                elif columns[key1] in market_list and columns[key2] not in market_list:
                    beta = cal_beta(x,y,xx,xy,num)
                    betas_tempdf = betas_dict.get(key3, betas_df)
                    betas_tempdf.ix[columns[key2],columns[key1]] = beta
                    betas_dict[key3] = betas_tempdf
                # use the last calculation result and the new value1, value2 to update the rolling correlations and betas
                key3 = line_index
                x_list.append(value1)
                y_list.append(value2)
                x_first = x_list.pop(0)
                y_first = y_list.pop(0)
                x += value1 - x_first
                y += value2 - y_first
                xx += value1*value1 - x_first*x_first
                yy += value2*value2 - y_first*y_first
                xy += value1*value2 - x_first*y_first
        # end of processing the current key
        else:
            # processing the last point of the current key
            if current_key:
                corr = cal_corr(x,y,xx,yy,xy,num)
                corr_tempdf = corr_dict.get(key3, corr_df)
                corr_tempdf.ix[key1,key2] = corr
                corr_tempdf.ix[key2,key1] = corr
                corr_dict[key3] = corr_tempdf
                # see if the symbol is the market observables
                if columns[key1] not in market_list and columns[key2] in market_list:
                    beta = cal_beta(y,x,yy,xy,num)
                    betas_tempdf = betas_dict.get(key3, betas_df)
                    betas_tempdf.ix[columns[key1],columns[key2]] = beta
                    betas_dict[key3] = betas_tempdf
                elif columns[key1] in market_list and columns[key2] not in market_list:
                    beta = cal_beta(x,y,xx,xy,num)
                    betas_tempdf = betas_dict.get(key3, betas_df)
                    betas_tempdf.ix[columns[key2],columns[key1]] = beta
                    betas_dict[key3] = betas_tempdf
            # reset for processing the new key
            current_key = key
            key1, key2 = key
            try:
                key1 = int(key1)
                key2 = int(key2)
            except ValueError:
                continue
            x_list = []
            y_list = []    
            x = value1
            y = value2
            xx = value1*value1
            yy = value2*value2
            xy = value1*value2
            num = 1
            key3 = line_index
    # process the last rolling 
    corr = cal_corr(x,y,xx,yy,xy,num)
    corr_tempdf = corr_dict.get(key3, corr_df)
    corr_tempdf.ix[key1,key2] = corr
    corr_tempdf.ix[key2,key1] = corr
    corr_dict[key3] = corr_tempdf
    # see if the symbol is the market observables
    if columns[key1] not in market_list and columns[key2] in market_list:
        beta = cal_beta(y,x,yy,xy,num)
        betas_tempdf = betas_dict.get(key3, betas_df)
        betas_tempdf.ix[columns[key1],columns[key2]] = beta
        betas_dict[key3] = betas_tempdf
    elif columns[key1] in market_list and columns[key2] not in market_list:
        beta = cal_beta(x,y,xx,xy,num)
        betas_tempdf = betas_dict.get(key3, betas_df)
        betas_tempdf.ix[columns[key2],columns[key1]] = beta
        betas_dict[key3] = betas_tempdf
    save_pkl()

def save_pkl():
    # column names and index names are all symbols
    for key, value in corr_dict.items():
        value.to_pickle('observations_'+str(key-num_observations+1)+':'+str(key)+'_correlations_matrix')
    for key, value in betas_dict.items():
        value.to_pickle('observations_'+str(key-num_observations+1)+':'+str(key)+'_betas_matrix')

if __name__ == '__main__':
    reducer()