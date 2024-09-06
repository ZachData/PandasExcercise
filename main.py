'''
Given client, date:
	return sum of value for purchase orders on that date.
	check to see if values were updated.

'''
import dataset
import read_data
import utils

import sys
import argparse
import pandas as pd
from abc import ABC, abstractmethod
from typing import Any, Optional

def parse_args(args):
	parser = argparse.ArgumentParser(
		description='Script to provide sums of EKPO[NETWR] given specific MANDT, EBELN, and D_DATE.'
		)
	parser.add_argument('--data_format', 
		default='easy', 
		type=str,
		choices=['easy', 'manual_entry', 'csv'],
		help="""How data will be input. Easy will return the literal *only* option for you. 
				csv for many entries. Manual entry for command line prompt. Please, pick easy.
				Str, Default = easy."""
		)
	parser.add_argument('--data_loc', 
		default='Data.csv',
		type=str,
		help="""Data location. Str, Default = 'Data.csv'. """
		)

	parser.add_argument('--verbose', 
		default=False,
		type=bool,
		help=""" If print methods should be enabled. Default = False.
				Gives lots of unneccesary information. """
		)

	parser.add_argument('--concat_path', 
		default=['EKKO', 'EKPO'], #[['E''K''K''O'], ['E''K''P''O']],
		nargs='+',
		type=list,
		help="""Path for data tables to traverse. Order does not affect output. Default=[EKKO, EKPO].
				Use format: --concat_path ABCD ABCD ABCD"""
		)

	return parser.parse_args()


# ==================================================================================
# Running check_data through list of desired tables
# ==================================================================================

class DataMethod(ABC):
	@abstractmethod
	def modification_method(self) -> pd.DataFrame:
		''' What is done to Request_table? '''
		pass

class AbstractDataMethod(DataMethod):
	#Making the initialization for the classes below.

	def __init__(self, request_table: pd.DataFrame, data_table: pd.DataFrame):

		self.request_table = request_table
		self.data_table = data_table

		comparisons = utils.CompareTableInformation(
			request_table = self.request_table,
			data_table = self.data_table			
			)
		self.matched_cols = comparisons.get_matching_cols()
		self.data_idx = comparisons.get_matched_index()

	def modification_method(self):
		''' What is done to Request_table? '''
		pass


class concat_data_method(AbstractDataMethod):
	''' Using a request table and a table composed of both CDPOS and CDHDR,
		update all avalable entries if the D_DATE is after their creation.  '''

	def modification_method(self) -> pd.DataFrame:
		''' Concat the new data from data_table to request_data '''
		concat = utils.ConcatTableInformation(
					request_table=self.request_table, 
					data_table=self.data_table
					)
		self.request_table = concat.concat_request_table(data_idx=self.data_idx)
		self.request_table = utils.delete_repeats(table=self.request_table)
		return self.request_table


class update_data_method(AbstractDataMethod):
	''' Using a request table and a table composed of both CDPOS and CDHDR,
		update all avalable entries if the D_DATE is after their creation.  '''
	
	def modification_method(self) -> pd.DataFrame:
		''' Use the update tables to update the request table '''

		update = utils.UpdateTableInformation(
					request_table=self.request_table, 
					data_table=self.data_table
					)
		# causal_table = update.find_causal_updates()
		self.request_table = update.remove_bad_date_rows()
		if self.request_table.empty:
			return []
		self.request_table = update.append_request_table()
		self.request_table = update.update_request_table()
		self.request_table = utils.delete_repeats(table=self.request_table)
		return self.request_table


def main():

	# make dataset
	data = dataset.makeDataSet()
	global EKKO, EKPO, CDPOS, CDHDR
	EKKO, EKPO, CDPOS, CDHDR = data.make_all()

	# parse entries
	args = parse_args(sys.argv[1:]) 

	# print dataset if verbose:
	if args.verbose:
		print('Dataset:')
		dataset.print_pretty() 

	# read in data
	if args.data_format == 'easy':
		ReadData = read_data.EasyData()
	elif args.data_format == 'csv':
		ReadData = read_data.ReadCSV(args.data_loc)
	elif args.data_format == 'manual_entry':
		ReadData = read_data.EnterData()

	# [['E', 'K', 'P', 'O'], ['E', 'K', 'K', 'O']]  -> ['EKPO', 'EKKO'] -> [EKPO, EKKO]
	path_raw = args.concat_path
	path_printable = [utils.fix_ugly_list(path_raw[i]) for i in range(len(path_raw))] 
	path = [globals()[utils.fix_ugly_list(path_raw[i])] for i in range(len(path_raw))] 
	
	# create requests, pass them through model
	Requests = ReadData.to_pandas()
	for request_idx in Requests.index:
		Request = Requests.iloc[[request_idx]]

		# concat request table
		print('Request:\n', Request, '\n')	
		for item in path:	
			instance = concat_data_method(request_table=Request, data_table=item)
			Request = instance.modification_method()
			if args.verbose:	
				print(f'Altered Request\n',  Request, '\n')

		# update request table
		update_tables = utils.merge_change_tables(CDHDR=CDHDR, CDPOS=CDPOS)
		if args.verbose:
			print('updates:\n', update_tables, '\n')
		instance = update_data_method(request_table=Request, data_table=update_tables)
		table = instance.modification_method()
		if args.verbose:	
			print('system output: \n', table, '\n')
		if len(table) == 0:
			continue

		# give the output sum.
		print(f"Chain: {path_printable} > Updates")
		sums = utils.sum_columns(table, 'NETWR')
		print('Sum:', sums)

		

if __name__ == "__main__":
	main()