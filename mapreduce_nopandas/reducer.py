#!/usr/bin/python
import sys
import numpy as np

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
	# set the number of obervations
	num_observations = 4
	market_list = [hash_function('S&P500'), hash_function('oil price')]
	# initialize current key
	current_key = None
	for line in parseInput():
		temp_key, value = line.strip().split('\t')
		temp_key1, temp_key2, time = temp_key.split(',')
		key = (temp_key1,temp_key2)
		value1, value2 = value.split(',')
		try:
			value1 = float(value1)
			value2 = float(value2)
		except ValueError:
			continue
		# calculate the first correlations matrix and betas using the previous methods
		if key == current_key:
			if current_observation < num_observations:
				x_list.append(value1)
				y_list.append(value2)
				x += value1
				y += value2
				xx += value1*value1
				yy += value2*value2
				xy += value1*value2
				num += 1
				current_observation += 1
				key3 = time
			# begins to process rolling observations
			else:
				corr = cal_corr(x,y,xx,yy,xy,num)
				print '%s,%s,%s,%s\t%.4f' % ('corr',key3,key1,key2,corr)
				# see if the symbol is the market observables
				if key1 not in market_list and key2 in market_list:
					beta = cal_beta(y,x,yy,xy,num)
					print '%s,%s,%s,%s\t%.4f' % ('beta',key3,key2,key1,beta)
				elif key1 in market_list and key2 not in market_list:
					beta = cal_beta(x,y,xx,xy,num)
					print '%s,%s,%s,%s\t%.4f' % ('beta',key3,key1,key2,beta)
				# use the last calculation result and the new value1, value2 to update the rolling correlations and betas
				x_list.append(value1)
				y_list.append(value2)
				x_first = x_list.pop(0)
				y_first = y_list.pop(0)
				x += value1 - x_first
				y += value2 - y_first
				xx += value1*value1 - x_first*x_first
				yy += value2*value2 - y_first*y_first
				xy += value1*value2 - x_first*y_first
				current_observation += 1
				key3 = time
		# end of processing the current key
		else:
			# processing the last point of the current key
			if current_key:
				corr = cal_corr(x,y,xx,yy,xy,num)
				print '%s,%s,%s,%s\t%.4f' % ('corr',key3,key1,key2,corr)
				# see if the symbol is the market observables
				if key1 not in market_list and key2 in market_list:
					beta = cal_beta(y,x,yy,xy,num)
					print '%s,%s,%s,%s\t%.4f' % ('beta',key3,key2,key1,beta)
				elif key1 in market_list and key2 not in market_list:
					beta = cal_beta(x,y,xx,xy,num)
					print '%s,%s,%s,%s\t%.4f' % ('beta',key3,key1,key2,beta)
			# reset for processing the new key
			current_key = key
			current_observation = 1
			# two symbols
			key1, key2 = key
			key3 = time
			x_list = []
			y_list = []    
			x = value1
			y = value2
			xx = value1*value1
			yy = value2*value2
			xy = value1*value2
			num = 1
	# process the last rolling 
	corr = cal_corr(x,y,xx,yy,xy,num)
	print '%s,%s,%s,%s\t%.4f' % ('corr',key3,key1,key2,corr)
	# see if the symbol is the market observables
	if key1 not in market_list and key2 in market_list:
		beta = cal_beta(y,x,yy,xy,num)
		print '%s,%s,%s,%s\t%.4f' % ('beta',key3,key2,key1,beta)
	elif key1 in market_list and key2 not in market_list:
		beta = cal_beta(x,y,xx,xy,num)
		print '%s,%s,%s,%s\t%.4f' % ('beta',key3,key1,key2,beta)

if __name__ == '__main__':
	reducer()