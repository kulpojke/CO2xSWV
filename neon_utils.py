import os
from glob import glob

def file_fetch(*args, path='./'):
    """
    returns a list of file names containing all args.  
    last arg should be the file extension
    """
    if not args:
        raise Exception('file_fetch expected some arguments but got none!')
    files = glob('{}**/NEON*{}'.format(path, args[-1]), recursive=True)
    for arg in args[0:-1]:
        yes = []
        for f in files:
            if arg in f:
                yes.append(f)
        files = yes
    return(files)
    # a faster version would build the glob string in a loop then glob at the end
    # but this allows args to be out of order save the extension           

def file_key_extract(file_list, keys):
    """
    returns a list of all the unique values of keys found in file_list.
    keys is a list of keys
    """
    set_of_keys = set()
    for f in file_list:
        set_of_keys |= set(get_file_keys(f, keys))
    return(list(set_of_keys))

def get_file_keys(f, keys):
    """ returns the file keys
        keys is a list """
    set_of_keys = set()
    file_keys = [
        'NEON', 'DOM', 'SITE', 'DPL', 'PRNUM', 'REV', 'HOR',
        'VER', 'TMI', 'DESC', 'YYYY-MM', 'PKGTYPE', 'GENTIME', 'csv']
    d = dict(zip(file_keys, f.split('.')))
    for k in keys:
            set_of_keys.add(d[k])
    return(list(set_of_keys))

def find_extension(ext, path='./'):
    """ returns a list of files with the extension ext, include the . in ext """
    files = os.listdir(path)    
    new_files = []
    for f in files:  
        if f.endswith(ext):
            new_files.append(f)
    return(new_files)

def make_header_list(PRNUM):
    headers_dict = {
        # Bulk secondary precipitation, millimeter OR 
        # Bulk throughfall precipitation,millimeter
        '00006': ['startDateTime', 'secPrecipBulk', 'secPrecipExpUncert', 'startDateTime', 'TFPrecipBulk', 'TFPrecipExpUncert'] , 
        # Arithmetic mean of Soil Temperature, celsius
        '00041': ['startDateTime', 'soilTempMean', 'soilTempStdErMean'],
        # Arithmetic mean of soil heat flux, wattsPerSquareMeter
        '00040': ['startDateTime', 'SHFMean', 'SHFStdErMean'],
        # Arithmetic mean of Soil CO2 concentration adjusted for temperature, pressure, 
        # oxygen, and humidity conditions, partsPerMillion
        '00095': ['startDateTime', 'soilCO2concentrationMean', 'soilCO2concentrationStdErMean'],
        # Arithmetic mean of volumetric soil water content, cm^3 per cm^3 OR 
        # Arithmetic mean of volumetric soil ion content, ,dimensionless
        '00094': ['startDateTime', 'VSWCMean', 'VSWCStdErMean', 'startDateTime', 'VSICMean', 'VSICStdErMean']
    }
    return(headers_dict[PRNUM])