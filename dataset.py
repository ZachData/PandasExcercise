'''
Here we deal with making the very strange dataset. 
'''
import numpy as np
import pandas as pd

class makeDataSet():
	'''Build the data needed.'''

	@staticmethod
	def make_EKKO():
		data_EKKO = {
			'MANDT' : ['100'],
			'EBELN' : ['1003789564'],
			'AEDAT' : ['01/01/2021']
			}
		EKKO = pd.DataFrame.from_dict(data_EKKO, orient='columns')
		for i in range(1, 200):
			fake_data = {
				'MANDT' : str(np.random.randint(high=999, low=101, size=1)[0]),
				'EBELN' : str(np.random.randint(high=999999999, low=10000000, size=1)[0]),
				'AEDAT' : (str(np.random.randint(high=30, low=1, size=1)[0]) + '/' + str(np.random.randint(high=12, low=1, size=1)[0]) + '/' + '2020' )
				}
			EKKO = EKKO.append(fake_data, ignore_index=True)
		return EKKO

	@staticmethod
	def make_EKPO():
		data_EKPO = {
			'MANDT' : ['100', '100', '100', '100'],
			'EBELN' : ['1003789564', '1003789564', '1003789564', '1003789564'],
			'EBELP' : ['000010', '000044', '000354', '009364'],
			'NETWR' : ['10.00', '44.44', '03.54', '93.64'],
			}
		EKPO = pd.DataFrame.from_dict(data_EKPO, orient='columns')
		for i in range(1, 200):
			fake_data = {
				'MANDT' : str(np.random.randint(high=999, low=101, size=1)[0]),
				'EBELN' : str(np.random.randint(high=999999999, low=10000000, size=1)[0]),
				'EBELP' : str(np.random.randint(high=999999, low=1, size=1)[0]),
				'NETWR' : str(np.random.randint(high=9999, low=1, size=1)[0] / 100),
				}
			EKPO = EKPO.append(fake_data, ignore_index=True)
		return EKPO

	@staticmethod
	def make_CDPOS():
		data_CDPOS_pricechange = {
			'MANDT'   : ['100', '100', ],
			'TABNAME'   : ['EKPO', 'EKPO',],
			'FNAME'     : ['NETWR', 'EBELP',],
			'TABKEY'    : ['1001003789564000010', '1001003789564000042',],
			'VALUE_OLD' : ['10.00', '',],
			'VALUE_NEW' : ['11.00', '000042',],
			'CHANGEID'  : ['U', 'I',],
			}
		data_CDPOS_itemchange = {
			'MANDT'     : '100', 
			'TABNAME'   : 'EKPO',
			'FNAME'     : 'EBELP',
			'TABKEY'    : '1001003789564000020',
			'VALUE_OLD' : '',
			'VALUE_NEW' : '000020',
			'CHANGEID'  : 'I',
			}
		data_CDPOS_itemchange2 = {
			'MANDT'     : '100',
			'TABNAME'   : 'EKPO',
			'FNAME'     : 'NETWR',
			'TABKEY'    : '1001003789564000020',
			'VALUE_OLD' : '',
			'VALUE_NEW' : '22.00',
			'CHANGEID'  : 'U',
			}
		data_CDPOS_itemchange3 = {
			'MANDT'     : '100',
			'TABNAME'   : 'EKPO',
			'FNAME'     : 'NETWR',
			'TABKEY'    : '1001003789564000042',
			'VALUE_OLD' : '',
			'VALUE_NEW' : '42.00',
			'CHANGEID'  : 'U',
			}
		CDPOS = pd.DataFrame.from_dict(data_CDPOS_pricechange, orient='columns')
		CDPOS = CDPOS.append(data_CDPOS_itemchange3, ignore_index=True)
		CDPOS = CDPOS.append(data_CDPOS_itemchange, ignore_index=True)
		CDPOS = CDPOS.append(data_CDPOS_itemchange2, ignore_index=True)
		return CDPOS

	@staticmethod
	def make_CDHDR():
		data_CDHDR = {
			'MANDT' : ['100', '100', '100', '100', '100'],
			'EBELN' : ['1003789564', '1003789564', '1003789564', '1003789564', '1003789564'],
			'CHANGENR' : ['01/02/2021', '07/02/2021', '09/02/2021', '01/03/2021', '01/03/2021'],
			}
		CDHDR = pd.DataFrame.from_dict(data_CDHDR, orient='columns')
		return CDHDR

	def make_all(self):
		EKKO = self.make_EKKO()
		EKPO = self.make_EKPO()
		CDPOS = self.make_CDPOS()
		CDHDR = self.make_CDHDR()
		return EKKO, EKPO, CDPOS, CDHDR


def print_pretty():
	instance = makeDataSet()
	EKKO, EKPO, CDPOS, CDHDR = instance.make_all()
	print("====================================================================")
	print('EKKO', '\n', EKKO, '\n')
	print('EKPO', '\n', EKPO, '\n')
	print('CDPOS', '\n', CDPOS, '\n')
	print('CDHDR', '\n', CDHDR, '\n')
	print("====================================================================")

