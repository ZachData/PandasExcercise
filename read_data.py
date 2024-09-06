
import pandas as pd
from abc import ABC, abstractmethod


class InputData(ABC):
	''' structure for all forms of data to be input'''
	@abstractmethod
	def to_pandas(self) -> pd.DataFrame:
		pass


class EasyData(InputData):
	''' data is automatically entered.'''
	def to_pandas(self) -> pd.DataFrame:
		data = {
			'MANDT' : ['100'],
			'EBELN' : ['1003789564'],
			'D_DATE' : ['01/01/2021']
			}
		df = pd.DataFrame.from_dict(data, orient='columns')
		return df
 

class ReadCSV(InputData):
	''' Load csv, grab MANDT, EBELN, D_DATE '''
	def __init__(self, data_loc):
		self.file = data_loc
	def to_pandas(self) -> pd.DataFrame:
		try:
			df = pd.read_csv(self.file, 
							names=['MANDT', 'EBELN', 'D_DATE'], 
							skipinitialspace=True, 
							dtype=str)
		except exception as e:
			print(e)
			print(r'Check that CSV has form: "MANDT", "EBELN", "D_DATE"')
		if df.isnull().values.any():
			print('\n', 'Some values are missing in the CSV. Those rows will be automatically deleted.', '\n')
			df = df.dropna(axis=0)

		print(df)
		return df

class EnterData(InputData):
	''' User enters MANDT, EBELN, D_DATE manually. '''
	def to_pandas(self) -> pd.DataFrame:
		MANDT = input('Please enter MANDT:')
		EBELN = input('Please enter EBELN:')
		D_DATE = input('Please enter D_DATE (dd/mm/yyyy):')

		data = {
			'MANDT' :  [MANDT],
			'EBELN' :  [EBELN],
			'D_DATE': [D_DATE],
			}
		df = pd.DataFrame(data=data, dtype=str)
		return df