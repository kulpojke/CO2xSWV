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
    '''TODO: make a docstring for this'''
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
                            j = f['name']
                            fname = data_path + f['name']
                            with open(fname, 'wb') as sink:
                                sink.write(handle.content)
                    
                    


#%%
sitecodes = ['BART', 'ABBY']
productcodes = ['DP1.00095.001', 'DP1.00094.001']
daterange = ['2014-12', '2020-11']
find_data(sitecodes, productcodes, daterange=daterange)

# %%


