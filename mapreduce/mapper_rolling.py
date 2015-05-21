#!/usr/bin/python
import sys, csv, StringIO, os
import pandas as pd
import numpy as np

def initialize():
	global num_observations
	global frequency
	global final_df
	# set the number of obervations
	num_observations = 6
	# set the default frequency
	frequency = 5
	try:
		# get records from previous results
		final_df = pd.read_pickle('return_dataframe')
	except IOError:
		final_df = pd.DataFrame([])

def parseInput():
	global num_observations
	global frequency
	global final_df
	global line_index
	# calculate correlations if there are more than one observations this time
	if (len(final_df.index)+1)/(frequency/5) >= num_observations:
		for k in range(0, len(final_df.index)):
			# exclude other observations other than frequency
			if k%(frequency/5) == 0:
				line_index = k/(frequency/5)
				line = final_df.ix[k,:]
				yield line

def mapper():
	global line_index
	# initialize the default values of number of observations and frequency
	initialize()
	# calculate correlations if there are more than one observations this time
	for record in parseInput():
		ncol = len(record)
		for i in range(0,ncol):
			key1 = i
			value1 = record[i]
			for j in range(i+1,ncol):
				key2 = j
				value2 = record[j]
				print '%d,%d,%d\t%s,%s' % (key1,key2,line_index,value1,value2)

if __name__ == '__main__':
	mapper()