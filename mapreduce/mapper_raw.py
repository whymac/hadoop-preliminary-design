#!/usr/bin/python
import sys, csv, StringIO, os

def parseInput():
	for line in sys.stdin:
		line = line.strip()
		csv_file = StringIO.StringIO(line)
		csv_reader = csv.reader(csv_file)
		for record in csv_reader:
			if len(record)>30 and record[0] != 'Date':
				yield record


# def parseInput():
# 	for line in sys.stdin:
# 		if os.environ["mapreduce_map_input_file"][-13:] == 'positions.csv':
# 			line = line.strip()
# 			csv_file = StringIO.StringIO(line)
# 			csv_reader = csv.reader(csv_file)
# 			for record in csv_reader:
# 				if record[0] != 'Date':
# 					time = record[1]
# 					symbol = record[8]
# 					# PnL_Ret = record[31]
# 					try:
# 						# total_market_pnl column divided by net_dollar_delta_current column
# 						PnL_Ret = float(record[38]) / float(record[20])
# 					except ZeroDivisionError:
# 						PnL_Ret = 0.0
# 					except ValueError:
# 						continue
# 					print '%s,%s\t%f' % (time, symbol, PnL_Ret)
# 		if os.environ["mapreduce_map_input_file"][-6:] == 'BU.csv':
# 			line = line.strip()
# 			csv_file = StringIO.StringIO(line)
# 			csv_reader = csv.reader(csv_file)
# 			for record in csv_reader:
# 				if record[0] != 'Date':
# 					time = record[1]
# 					BU = record[3]
# 					PnL_Ret = record[9].strip('%')
# 					try:
# 						PnL_Ret = float(PnL_Ret)
# 					except ValueError:
# 						PnL_Ret = 0.0
# 					# record 
# 					print '%s,%s\t%f' % (time, BU, PnL_Ret)

def mapper():
	# initialize the default values of number of observations and frequency
	for record in parseInput():
		#if os.environ["mapreduce_map_input_file"][-13:] == 'positions.csv':
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
		print '%s,%s\t%f' % (time, symbol, PnL_Ret)

if __name__ == '__main__':
	mapper()