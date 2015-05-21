#!/usr/bin/python
import sys
import pandas as pd
import numpy as np

def initialize():
	global corr_df, betas_df
	global current_key, columns, market_list
	#initialization
	current_key = None
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

def save_pkl():
	# column names and index names are all symbols
	corr_df.to_pickle('correlations_matrix')
	betas_df.to_pickle('betas_matrix')

def reducer():
	global corr_df, betas_df
	global current_key, columns, market_list
	# initialization
	initialize()
	for line in parseInput():
		key, value = line.strip().split('\t')
		value1, value2 = value.split(',')
		try:
			value1 = float(value1)
			value2 = float(value2)
		except ValueError:
			continue
		if key == current_key:
			x += value1
			y += value2
			xx += value1*value1
			yy += value2*value2
			xy += value1*value2
			num += 1
		else:
			if current_key:
				corr = cal_corr(x,y,xx,yy,xy,num)
				corr_df.ix[key1,key2] = corr
				corr_df.ix[key2,key1] = corr
				# see if the symbol is the market observables
				if columns[key1] not in market_list and columns[key2] in market_list:
					beta = cal_beta(y,x,yy,xy,num)
					betas_df.ix[columns[key1],columns[key2]] = beta
				elif columns[key1] in market_list and columns[key2] not in market_list:
					beta = cal_beta(x,y,xx,xy,num)
					betas_df.ix[columns[key2],columns[key1]] = beta
			current_key = key
			key1, key2 = key.split(',')
			try:
				key1 = int(key1)
				key2 = int(key2)
			except ValueError:
				continue		
			x = value1
			y = value2
			xx = value1*value1
			yy = value2*value2
			xy = value1*value2
			num = 1
	# process the last key  
	corr = cal_corr(x,y,xx,yy,xy,num)
	corr_df.ix[key1,key2] = corr
	corr_df.ix[key2,key1] = corr
	# see if the symbol is the market observables
	if columns[key1] not in market_list and columns[key2] in market_list:
		beta = cal_beta(y,x,yy,xy,num)
		betas_df.ix[columns[key1],columns[key2]] = beta
	elif columns[key1] in market_list and columns[key2] not in market_list:
		beta = cal_beta(x,y,xx,xy,num)
		betas_df.ix[columns[key2],columns[key1]] = beta
	save_pkl()

if __name__ == '__main__':
	reducer()