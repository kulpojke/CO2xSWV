import pandas as pd
import requests
import hashlib
import os
import glob

from dask import delayed
import dask
from dask.diagnostics import ProgressBar


# ------------------------------------------------------------------------------------
# functions related to downloading from the API



def fetch_data_from_NEON_API(sitecodes, productcodes, daterange = 'most recent', data_path='/home/jovyan/NEON/CO2xSWV_data'):
    '''TODO: make a docstring for this, and move it to neon_utils when all done.
    
    '''
    base_url = 'https://data.neonscience.org/api/v0/'
    data_path = data_path.rstrip('/') + '/'
    lazy = []
    for site in sitecodes:
        #this part determines which dates are available for the site/product
        dates = get_common_dates(site, productcodes, base_url)
        if daterange == 'most recent':
            # get the most recent date
            dates = [max(dates)]
        elif daterange == 'all':
            pass
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
            for product in productcodes:
                try:
                    sensor_positions(product, site, date, data_path)
                except:
                    pass
                result = delayed(dload)(product, site, date, base_url, data_path)
                lazy.append(result)
    with ProgressBar():
        dask.compute(*lazy)
        
        
        
def get_common_dates(site, products, base_url):        
    dates_list = []
    for product in products:
        #this part determines which dates are available for the site/product
        url = f'{base_url}sites/{site}'
        response = requests.get(url)
        data = response.json()['data']
        dates = set(data['dataProducts'][0]['availableMonths'])
        dates_list.append(dates)
    dates = list(set.intersection(*dates_list))
    return(dates)
        
        
        
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
                try:
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
                except Exception as e:
                    print(f'Warning:\n{e}')
                    success = False
                    attempts = attempts + 1
            # write the file
            if success:
                fname = data_path + f['name']
                with open(fname, 'wb') as sink:
                    sink.write(handle.content)


def sensor_positions(product, site, date, data_path):
    attempts = 0
    success = False
    while (attempts < 4) & (success == False):
        success = download_sensor_positions(product, site, date, data_path)
        attempts = attempts + 1
        

def download_sensor_positions(product, site, date, data_path):
    # find the url and name of sensor_positions file
    path = data_path.rstrip('/')
    base_url = 'https://data.neonscience.org/api/v0/'
    url = f'{base_url}data/{product}/{site}/{date}'
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f'Bad {url} returns {response.statuscode}')
    name, url, md5 = find_sensor_positions_url(response)
    # download and save the sensor positions file 
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f'Bad url for {name}')
    # check the md5
    if md5 == hashlib.md5(response.content).hexdigest():
        fname = path + '/' + name
        with open(fname, 'wb') as sink:
            sink.write(response.content)
            return(True)
    else:
        return(False)
        
    
def find_sensor_positions_url(response):
    '''Find url for sensor_positions file from NEON API response'''
    data = response.json()['data']
    for f in data['files']:
        if 'sensor_positions' in f['name']:
            return(f['name'], f['url'], f['md5'])               
    raise Exception('No sensor_positions file!')

# ------------------------------------------------------------------------------------    
# functions related to selecting data and creating dataframes


def find_HOR_VER(site, data_path):
    # glob files for the site
    data_path='/home/jovyan/NEON/CO2xSWV_data'
    minute = '[0-9]' * 3 + '.' + '[0-9]' * 3 + '.001' 
    soil_CO2 = glob.glob(f'{data_path}/*{site}.DP1.00095.001.{minute}.*csv')
    soil_H2O = glob.glob(f'{data_path}/*{site}.DP1.00094.001.{minute}.*csv')
    soil_T   = glob.glob(f'{data_path}/*{site}.DP1.00041.001.{minute}.*csv')
    # make lists with date, and files for that date
    sc = set([f.split('.')[-4] for f in soil_CO2])
    sw = set([f.split('.')[-4] for f in soil_H2O])
    st = set([f.split('.')[-4] for f in soil_T])
    # find dates present in all data products
    dates = list(sc & sw & st)
    dates.sort()
    horvers = []
    
    for date in dates:
        # make seperate lists of each product for the dates where all are present
        cc = [f for f in soil_CO2 if date in f]
        ww = [f for f in soil_H2O if date in f]
        tt = [f for f in soil_T if date in f]
        #find HOR and VER combinations for the date (e.g. '003501')
        ccc = [''.join(f.split('.')[6:8]) for f in cc]
        www = [''.join(f.split('.')[6:8]) for f in ww]
        ttt = [''.join(f.split('.')[6:8]) for f in tt]
        horver = list(set(ccc) & set(www) & set(ttt))
        horvers.append(horver)

    horver = set(horvers[0])   
    for hv in horvers[1:]:
        horver = horver & set(hv)
    horver = list(horver)

    hor = list(set([hv[:3] for hv in horver]))
    hor.sort()
    hor_ver = [[hv[:3], hv[3:]] for hv in horver]
    horver = {key : [] for key in hor}
    for key, val in hor_ver:
        horver[key].append(val)

    # horver is a dict all HOR-VER combinations available at  site
    # its like {Hor : [z1, z2,...]}
    return(horver)      
    
  

def make_df(hor, ver, date, site, data_path):
    '''Reads  NEON 1 minute cvs for:
           DP1.00094.001  (Soil CO2 concentrations)
           DP1.00041.001  (Soil Temperature)
           DP1.00095.001  (soil volumetric water content and salintiy)
       drops entries with bad finalQF flags,
       drops quality metric columns,
       and returns a  dataframe of merged data.
    
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
    minute = '[0-9]' * 3 + '.' + '[0-9]' * 3 + '.001' 
    co2 = glob.glob(f'{data_path}/*{site}.DP1.00095.001.{minute}.*csv')[0]
    h2o = glob.glob(f'{data_path}/*{site}.DP1.00094.001.{minute}.*csv')[0]
    t   = glob.glob(f'{data_path}/*{site}.DP1.00041.001.{minute}.*csv')[0]

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
    print(co2.info())

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
    print(h2o.info())

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
    print(soil_T.info())
    
    # merge co2, h2o, soil_T into one df
    l1 = len(co2)
    co2 = co2.merge(h2o, left_index=True, right_index=True)
    l2 = len(co2)
    co2 = co2.merge(soil_T, left_index=True, right_index=True)
    l3 = len(co2)
    print(f'{l1} --> {l2} --> {l3}')

    if len(co2) > 0:
        co2 = co2.resample('1h').mean()
           
    return(co2)

# ------------------------------------------------------------------------------------

