#!/usr/bin/python
import sys, csv, StringIO, os
from datetime import datetime as dt

def parseInput():
	for line in sys.stdin:
		if os.environ["mapreduce_map_input_file"][-13:] == 'positions.csv':
			line = line.strip()
			csv_file = StringIO.StringIO(line)
			csv_reader = csv.reader(csv_file)
			for record in csv_reader:
				if len(record)>30 and record[0] != 'Date':
					yield record

def time_diff(time1,time2):
    time1 = dt.strptime(time1,'%H%M%S')
    time2 = dt.strptime(time2,'%H%M%S')
    diff = time1 - time2
    return diff.seconds

def mapper():
	# set the default frequency
	frequency = 5
	# initialize current time
	current_time = None
	for record in parseInput():
		time = record[1]
		symbol = record[8]
		# PnL_Ret = record[31]
		try:
		# total_market_pnl column divided by net_dollar_delta_current column
			PnL_Ret = float(record[38]) / float(record[20])
		except ZeroDivisionError:
			PnL_Ret = 0.0
		except ValueError:
			continue
		if time == current_time:			
			symbol_dict[symbol] = PnL_Ret
		# process first line
		elif not current_time:
			current_time = time
			symbol_dict = {}
		# apply frequency
		elif time_diff(time, current_time) == (frequency*60):
			ncol = len(symbol_dict)
			for i in range(0,ncol):
				key1 = symbol_dict.keys()[i]
				value1 = symbol_dict.get(key1,0.0)
				for j in range(i+1,ncol):
					key2 = symbol_dict.keys()[j]
					value2 = symbol_dict.get(key2,0.0)
					print '%s,%s,%s\t%s,%s' % (key1,key2,current_time,value1,value2)
			current_time = time
			symbol_dict = {}
	# process the last line
	ncol = len(symbol_dict)
	for i in range(0,ncol):
		key1 = symbol_dict.keys()[i]
		value1 = symbol_dict.get(key1,0.0)
		for j in range(i+1,ncol):
			key2 = symbol_dict.keys()[j]
			value2 = symbol_dict.get(key2,0.0)
			print '%s,%s,%s\t%s,%s' % (key1,key2,current_time,value1,value2)

if __name__ == '__main__':
	mapper()