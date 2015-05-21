#!/usr/bin/python
import sys
import pandas as pd
import numpy as np

def initialize():
	global current_key
	global current_time
	global current_df
	#initialization
	current_key = None
	current_time = None
	try:
		# get records from previous results
		current_df = pd.read_pickle('return_dataframe')
	except IOError:
		current_df = pd.DataFrame([])

def parseInput():
	for line in sys.stdin:
		yield line.strip()

def save_pkl():
	# column names are are all symbols, index names are observation times
	final_df.to_pickle('return_dataframe')

def reducer():
	# input comes from STDIN (stream data that goes to the program)
	global current_key
	global current_time
	global current_df
	global final_df
	#initialization
	initialize()
	for line in parseInput():
		key, value = line.strip().split('\t')
		time, symbol = key.split(',')
		try:
			PnL_Ret = float(value)
		except ValueError:
			continue
		# test if it is the same time same symbol
		if key == current_key:
			# if the same symbol appears more than once, take the average of %return
			duplicate_Ret.append(PnL_Ret)
		# test if it is the same time another symbol
		elif time == current_time:
			# put previous symbol into dict
			pair[current_symbol] = np.mean(duplicate_Ret)
			# reset
			duplicate_Ret = [PnL_Ret]
			current_key = key
			current_symbol = symbol
		# test if it is the first line
		else:
			# test if it is the first line
			if current_key:
				# put previous symbol into dict
				pair[current_symbol] = np.mean(duplicate_Ret)
				# insert dict as a new line into the previous dataframe
				temp_df = pd.DataFrame(pair, index = [current_time])
				current_df = pd.concat([current_df,temp_df])
			# reset for next observation if mapper process more than one observation
			pair =  {}
			duplicate_Ret = [PnL_Ret]
			current_key = key
			current_time = time
			current_symbol = symbol
	# process the last symbol
	pair[current_symbol] = np.mean(duplicate_Ret)
	# insert dict as a new line into the previous dataframe
	temp_df = pd.DataFrame(pair, index = [current_time])
	# prevent if in one observation there more or less positions than previous observations
	final_df = pd.concat([current_df,temp_df]).fillna(0.0)
	save_pkl()

if __name__ == '__main__':
	reducer()
