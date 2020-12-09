#/* cSpell:disable */
#%%
import pandas as pd
import requests
import hashlib
import os
from tqdm import tqdm  
import glob

# %%
def fetch_data_from_NEON_API(sitecodes, productcodes, daterange = 'most recent', data_path='/media/data/NEON/CO2xSWV'):
    '''TODO: make a docstring for this, and move it to neon_utils when all done'''
    base_url = 'https://data.neonscience.org/api/v0/'
    data_path = data_path.rstrip('/') + '/'
    for site in sitecodes:
        for product in productcodes:
            #this part determines which dates are available for the site/product
            url = f'{base_url}sites/{site}'
            response = requests.get(url)
            data = response.json()['data']
            dates = data['dataProducts'][0]['availableMonths']
            if daterange == 'most recent':
                # get the most recent date
                dates = [max(dates)]
            else:
                try:
                    # get dates in the range
                    assert isinstance(daterange,list)
                    begin, terminate = min(daterange), max(daterange)
                    dates = [d  for d in dates if (d >= begin) and (d <= terminate)]                 
                except AssertionError:
                    print('daterange must be a list, e.g. [\'2020-10\', \'2019-10\']')
                    return(None)

            for date in dates:
                print(f'{product}-{site}-{date}') 
                url = f'{base_url}data/{product}/{site}/{date}'
                response = requests.get(url)
                data = response.json()
                files = data['data']['files']
                os.makedirs(data_path, exist_ok=True)
                for f in tqdm(files):
                    if ('expanded' in f['name']) & ('_1' in f['name']) & (f['name'].endswith('.csv')):                        
                        attempts = 0 
                        while attempts < 4:
                            # get the file 
                            handle = requests.get(f['url'])
                            #check the md5
                            md5 = hashlib.md5(handle.content).hexdigest()
                            if md5 == f['md5']:
                                success = True
                                attempts = 4
                            else:
                                print(f'md5 mismatch on attempt {attempts}')
                                success = False
                                attempts = attempts + 1
                        # write the file
                        if success:
                            fname = data_path + f['name']
                            with open(fname, 'wb') as sink:
                                sink.write(handle.content)
                    
                    


#%%
sitecodes = ['BART', 'ABBY']
productcodes = ['DP1.00095.001', 'DP1.00094.001']
daterange = ['2020-09', '2020-11']
fetch_data_from_NEON_API(sitecodes, productcodes, daterange=daterange)

# %%
data_path='/media/data/NEON/CO2xSWV'

site = 'ABBY'
soil_CO2 = glob.glob(f'{data_path}/*{site}*SCO2C_1_minute*.csv')
soil_H2O = glob.glob(f'{data_path}/*{site}*SWS_1_minute*.csv')

# make a df with date, and files for that date
sc = set([f.split('.')[-4] for f in soil_CO2])
sw = set([f.split('.')[-4] for f in soil_H2O])
dates = list(sc & sw)
dates.sort()
c = []
w = []
for date in dates:
    # each comprehension should return 1 filename
    c.append([f for f in soil_CO2 if date in f][0])
    w.append([f for f in soil_H2O if date in f][0])
filedf = pd.DataFrame()
filedf['date'] = dates
filedf['CO2']  = c
filedf['H2O']  = w


# %%
#for date in dates:
#   row = filedf.loc[filedf.date==date]
#   f = row['CO2'] 
f = c[10]
co2 = pd.read_csv(f, parse_dates=True, index_col='startDateTime')
# Fail and pass columns are redundent, we will use the fails
drops = [col for col in list(co2.columns) if 'Pass' in col]
co2.drop(drops, axis='columns', inplace=True)
# drop columns with bad quality flags
x = len(co2)
co2 = co2.loc[co2.finalQF == 0] 
#if (x - len(co2)) / x > 0.2: continue
print(f'dropped {100 * (x - len(co2)) / x}%')
# now drop quality metric columns
qm = [col for col in list(co2.columns) if 'QM' in col]
qm = qm + [col for col in list(co2.columns) if 'QF' in col] + ['endDateTime']
co2.drop(qm, axis='columns', inplace=True)
#this following step should not be needed, but just in case
co2.dropna(inplace=True)


#%%
#   f = row['H2O']
f = w[-1]
h2o = pd.read_csv(f, parse_dates=True, index_col='startDateTime')
# Fail and pass columns are redundent, we will use the fails
drops = [col for col in list(h2o.columns) if 'Pass' in col]
h2o.drop(drops, axis='columns', inplace=True)
# drop columns with bad quality flags
x = len(h2o)
h2o = h2o.loc[(h2o.VSWCFinalQFSciRvw != 2) & (h2o.VSICFinalQFSciRvw != 2)]
#if (x - len(h2o)) / x > 0.2: continue
print(f'dropped {100 * (x - len(h2o)) / x}%\nbut note that we are using FinalQFSciRvw != 2 rather than using FinalQF == 0 as our loc criteria')
# now drop quality metric columns
qm = [col for col in list(h2o.columns) if 'QM' in col]
qm = qm + [col for col in list(h2o.columns) if 'QF' in col] + ['endDateTime']
h2o.drop(qm, axis='columns', inplace=True)
#this following step should not be needed, but just in case
h2o.dropna(inplace=True)

# %%
