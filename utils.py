'''
A place to find the general methods.
'''

import dataset
import read_data

import numpy as np
import pandas as pd
from datetime import datetime
from abc import ABC, abstractmethod
from decimal import *

# ==================================================================================
# Helper Functions
# ==================================================================================

def if_equal_dates(table: pd.DataFrame) -> pd.DataFrame:
	''' helper function to compare dates. Use two column dataframe, use apply method.'''
	first_date = datetime.strptime(table[0], "%d/%m/%Y")
	second_date = datetime.strptime(table[1], "%d/%m/%Y")
	return first_date.date() == second_date.date()


def delete_repeats(table: pd.DataFrame) -> pd.DataFrame:
	''' Assumes that all information already exists in table. 
		Ex: if MANDT nan, filling cannot cause a mistake. 
			If NETWR nan, filling will likely lead to an error. This CANNOT be allowed to happen. 
				With the current dataset, an error here is impossible. '''
	table = table.fillna(method='ffill')
	table = table.fillna(method='bfill')
	table = table.iloc[:,~table.columns.duplicated()]
	table = table.drop_duplicates()
	return table


def merge_change_tables(CDHDR: pd.DataFrame, CDPOS: pd.DataFrame) -> pd.DataFrame:
	''' Concat CDHDR and CDPOS to make one coherent update table. '''
	assert len(CDHDR) == len(CDPOS), 'The lengths of the update tables are not the same.' # we assume 1:1 and correct.
	merged_table = pd.concat([CDPOS, CDHDR], axis=1)
	merged_table = merged_table.T.drop_duplicates().T

	len_table = len(merged_table['MANDT'])
	len_mandt_vals = len(merged_table['MANDT'][0])
	len_ebeln_vals = len(merged_table['EBELN'][0])
	
	EBELP = []
	for idx in range(0, len_table):
		assert merged_table['MANDT'][idx] == merged_table['TABKEY'][idx][0:len_mandt_vals], 'MANDT and TABKEY are not the same.' # we assume
		assert merged_table['EBELN'][idx] == merged_table['TABKEY'][idx][len_mandt_vals:len_mandt_vals+len_ebeln_vals], 'EBELN and TABKEY are not the same.' # we assume
		EBELP.append(merged_table['TABKEY'][idx][len_mandt_vals+len_ebeln_vals:])

	merged_table = pd.concat([merged_table, pd.DataFrame({'EBELP':EBELP})], axis=1)
	return merged_table


# ==================================================================================
# String Manipulation
# ==================================================================================

def fix_ugly_list(ugly_list: list) -> list:
	''' ['E', 'K', 'P', 'O'] -> 'EKPO' '''
	new_list = ""
	for character in ugly_list:
		new_list += character 
	return new_list

# ==================================================================================
# Summing Columns
# ==================================================================================

def to_type_decimal(value: float) -> Decimal: 
	''' for use with df.apply methods ''' 
	return Decimal(value)

def sum_columns(table: pd.DataFrame, col: str) -> Decimal:
	''' sum a specific column using the decimal library '''
	getcontext().prec = 64 # prec is precision
	decimal_col = table[col]
	sum_col = decimal_col.apply(to_type_decimal)
	sums = sum_col.sum()
	return sums


# ==================================================================================
# Matching Tables
# ==================================================================================

class CompareTableInformation():
	''' if tables have information in common, find the associated indexes (idx) with the data tables to use later. '''

	def __init__(self, request_table: pd.DataFrame, data_table: pd.DataFrame):
		self.request_table = request_table
		self.data_table = data_table

	def get_matching_cols(self) -> list:
		'''find matching columns in dataframe, return them as list.'''
		return [column for column in self.request_table if column in self.data_table]

	def get_matched_index(self) -> list:
		''' Find the row or rows (index) associated with the process order. '''
		matching_cols = self.get_matching_cols()
		request_subset = self.request_table[matching_cols]
		data_subset = self.data_table[matching_cols]

		# for each request subset, find row that agrees in data table subset and log its index
		full_idx = []
		for req_idx in range(len(request_subset)):
			data_idx = []
			for idx in range(len(data_subset.index)):
				if request_subset.empty or request_subset.isna().any(axis=1).values[0]:
					continue
				noNAN = data_subset.iloc[[idx]].notna().any(axis=1).all()
				is_match = (data_subset.iloc[[idx]].values == request_subset.iloc[[req_idx]].values)
				is_match = is_match.all(axis=1) # [True, True] -> True
				if is_match and noNAN:
					data_idx.append(idx)
			full_idx.append(data_idx)

		assert len(np.array(full_idx, dtype=object).flatten()) != 0, 'There is no relationship between the data and table given.'
		return full_idx 



# ==================================================================================
# Concat to Request Table 
# ==================================================================================

class ConcatTableInformation():
	''' if tables have information in common, add the information NOT in common to the request table. '''

	def __init__(self, request_table: pd.DataFrame, data_table: pd.DataFrame):
		self.request_table = request_table
		self.data_table = data_table

	def concat_request_table(self, data_idx) -> pd.DataFrame:
		''' [req_rows, N] -> [req_rows, M] -> [unique(req_rows, data_rows), M]'''

		# input should be list of lists, to accomodate multiple request rows.
		data_idx = np.array(data_idx).flatten()

		#get relevent data, append to req table
		datatable_onlygoodidx = self.data_table.iloc[data_idx]
		first_row = pd.concat([self.request_table.iloc[[0]], datatable_onlygoodidx], 
							 axis=1
							 )
		output = first_row.fillna(method="ffill")
		output = output.iloc[:,~output.columns.duplicated()]
		if len(self.request_table.index) > 1:
			output = output.reset_index(drop=True)
			output.update(self.request_table)
		return output

# ==================================================================================
# Append and Update:
# ==================================================================================

class UpdateTableInformation():
	''' if there are updates which existed before D_DATE, find and implement them. '''

	def __init__(self, request_table: pd.DataFrame, data_table: pd.DataFrame):
		self.request_table = request_table
		self.data_table = data_table

	def remove_bad_date_rows(self) -> pd.DataFrame:
		''' If D_date is not on PO creation date, delete that row. '''
		
		# find causal idx
		is_causal = self.request_table[['AEDAT', 'D_DATE']].apply(if_equal_dates, axis=1)
		if is_causal.all() == False: 
			print('No valid PO forms found on D_DATE')

		# if causal, remove from table
		true_idx = is_causal.index[is_causal == True].tolist()
		causal_request_table = self.request_table.iloc[true_idx]		
		return causal_request_table

	def append_request_table(self) -> pd.DataFrame: 
		''' append to request table from data table '''
		
		# create update table
		append_idx = self.data_table.index[self.data_table['CHANGEID'] == 'I'].tolist() # (I)nsert
		append_table = self.data_table.iloc[append_idx]
		appends_as_np = append_table[['FNAME', 'VALUE_NEW']].values 

		# from update_table, if new value not in req table: append and fill nan
		for col, new_val in appends_as_np:
			if self.request_table[col].isin([new_val]).any() == True:
				continue
			self.request_table = self.request_table.append(pd.Series({col:new_val}), ignore_index=True)
		self.request_table = self.request_table.fillna(method='ffill')
		return self.request_table

	def update_request_table(self) -> pd.DataFrame:
		''' update request table based on the merged update table. '''
		
		# idx to upd
		update_idx = self.data_table.index[self.data_table['CHANGEID'] == 'U'].tolist() # (U)pdate
		update_table = self.data_table.iloc[update_idx].reset_index(drop=True)
		update_as_np = update_table[['FNAME', 'VALUE_NEW']].values 

		# if cols match:
		matching_idx = CompareTableInformation(update_table, self.request_table).get_matched_index()
		matching_idx = np.array(matching_idx)

		# update fname col to new_value
		index = 0
		for req_idx in matching_idx:
			col = update_table.at[index, 'FNAME']
			new_val = update_table.at[index, 'VALUE_NEW']
			self.request_table.at[req_idx[0], col] = new_val
			index += 1				

		return self.request_table	
