#!/usr/bin/python
import sys, csv, StringIO, os
import pandas as pd
import numpy as np

def parseInput():
	global final_df
	try:
		# get records from previous results
		final_df = pd.read_pickle('return_dataframe')
	except IOError:
		final_df = pd.DataFrame([])
	# calculate correlations if there are more than one observations this time
	for k in range(0, len(final_df.index)):
		# if the return dataframe is saved into csv file
		line = final_df.ix[k,:]
	yield line

def mapper():
	# initialize the default values of number of observations and frequency
	for record in parseInput():
		ncol = len(record)
		for i in range(0,ncol):
			key1 = i
			value1 = record[i]
			for j in range(i+1,ncol):
				key2 = j
				value2 = record[j]
				print '%d,%d\t%s,%s' % (key1,key2,value1,value2)

if __name__ == '__main__':
	mapper()