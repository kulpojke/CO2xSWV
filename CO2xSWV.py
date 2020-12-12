import pandas as pd
import requests
import hashlib
import os
import glob

from dask import delayed
import dask
from dask.diagnostics import ProgressBar

def fetch_data_from_NEON_API(sitecodes, productcodes, daterange = 'most recent', data_path='/home/jovyan/NEON/CO2xSWV_data'):
    '''TODO: make a docstring for this, and move it to neon_utils when all done.
    
    '''
    base_url = 'https://data.neonscience.org/api/v0/'
    data_path = data_path.rstrip('/') + '/'
    lazy = []
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
                result = dload(product, site, date, base_url, data_path)
                lazy.append(result)
    with ProgressBar():
        dask.compute(*lazy)
        
@delayed                
def dload(product, site, date, base_url, data_path):                     
    url = f'{base_url}data/{product}/{site}/{date}'
    response = requests.get(url)
    data = response.json()
    files = data['data']['files']
    os.makedirs(data_path, exist_ok=True)
    for f in files:
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

# ------------------------------------------------------------------------------------
                    
@delayed
def make_df(hor, ver, date, site, data_path):
    '''Reads  NEON 1 minute cvs for:
           DP1.00094.001  (Soil CO2 concentrations)
           DP1.00041.001  (Soil Temperature)
           DP1.00095.001  (soil volumetric water content and salintiy)
       drops entries with bad finalQF flags,
       drops quality metric columns,
       and returns a delayed dataframe of merged data
       (use dask.compute() to make into pandas df).
    
    Arguments:
    hor  -- String - horizontal sensor position (HOR in the 
            NEON product readme files).
    ver  -- String - vertical sensor position   (VER in the 
            NEON product readme files).
    date -- String - month of data desired. (yyyy-mm)
    site -- String - NOEN site code (e.g. 'BART')
    data_path -- String - path to data.
    '''
    # glob the filenames, only one gets globbed for each
    hv_glob = hor + '.' + ver + '.' + ('[0-9]' * 3 )
    co2 = glob.glob(f'{data_path}/*{site}*{hv_glob}*SCO2C_1_minute.{date}*.csv')[0]
    h2o = glob.glob(f'{data_path}/*{site}*{hv_glob}*SWS_1_minute.{date}*.csv')[0]
    t   = glob.glob(f'{data_path}/*{site}*{hv_glob}*ST_1_minute.{date}*.csv')[0]

    # make CO2 df
    co2 = pd.read_csv(co2, parse_dates=True, index_col='startDateTime')
    # Fail and pass columns are redundent, we will use the fails
    drops = [col for col in list(co2.columns) if 'Pass' in col]
    co2.drop(drops, axis='columns', inplace=True)
    # drop columns with bad quality flags
    x = len(co2)
    co2 = co2.loc[co2.finalQF == 0]
    # now drop quality metric columns
    qm = [col for col in list(co2.columns) if 'QM' in col]
    qm = qm + [col for col in list(co2.columns) if 'QF' in col] + ['endDateTime']
    co2.drop(qm, axis='columns', inplace=True)
    #this following step should not be needed, but just in case
    co2.dropna(inplace=True)

    # make H2O df
    h2o = pd.read_csv(h2o, parse_dates=True, index_col='startDateTime')
    # Fail and pass columns are redundent, we will use the fails
    drops = [col for col in list(h2o.columns) if 'Pass' in col]
    h2o.drop(drops, axis='columns', inplace=True)
    # drop columns with bad quality flags
    x = len(h2o)
    h2o = h2o.loc[(h2o.VSWCFinalQF == 0) & (h2o.VSICFinalQF == 0)]
    # now drop quality metric columns
    qm = [col for col in list(h2o.columns) if 'QM' in col]
    qm = qm + [col for col in list(h2o.columns) if 'QF' in col] + ['endDateTime']
    h2o.drop(qm, axis='columns', inplace=True)
    #this following step should not be needed, but just in case
    h2o.dropna(inplace=True)

    # make soil temp df
    soil_T = pd.read_csv(t, parse_dates=True, index_col='startDateTime')
    # Fail and pass columns are redundent, we will use the fails
    drops = [col for col in list(soil_T.columns) if 'Pass' in col]
    soil_T.drop(drops, axis='columns', inplace=True)
    # drop columns with bad quality flags
    x = len(soil_T)
    soil_T = soil_T.loc[soil_T.finalQF == 0]
    # now drop quality metric columns
    qm = [col for col in list(soil_T.columns) if 'QM' in col]
    qm = qm + [col for col in list(soil_T.columns) if 'QF' in col] + ['endDateTime']
    soil_T.drop(qm, axis='columns', inplace=True)
    #this following step should not be needed, but just in case
    soil_T.dropna(inplace=True)
    
    # merge co2, h2o, soil_T into one df
    co2 = co2.merge(h2o, left_index=True, right_index=True)
    co2 = co2.merge(soil_T, left_index=True, right_index=True)
    
    if len(co2) > 0:
        co2 = co2.resample('1h').mean()
           
    return(co2)

# ------------------------------------------------------------------------------------

