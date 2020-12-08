from neon_utils import find_extension
import os
from shutil import copyfile
from shutil import rmtree, copyfile, copytree
import zipfile



def extract_to_dir(source_path, target_path, zips='all'):
    '''extracts specified zips from source_path to target_path.
    if zips is not specified extracts all zips found in source_path '''
    zip_keys = [
        'NEON', 'DOM', 'SITE', 'DPL', 'PRNUM', 'REV',
        'YYYY-MM', 'PKGTYPE', 'GENTIME', 'zip']
    extract_move_meta(zip_keys=zip_keys, source_path=source_path, target_path=target_path, zips=zips)

def extract_move_meta(zip_keys, source_path, target_path, zips='all'):
    if zips == 'all':
        zips = [source_path + f for f in find_extension('.zip', path=source_path)]
    else:
        zips = [source_path + f for f in zips]
    dirs = [target_path] * len(zips)
    zf_dict = dict(zip(zips, dirs))
    unzipper(zf_dict)
    print('outer unzipped')
    # the above code should have unzipped the outer zips into
    pdfs = find_extension('.pdf', path=target_path)
    try:
        rmtree('../PDFs')
        os.mkdir('../PDFs')
    except FileNotFoundError:
        os.mkdir('../PDFs')
    for pdf in pdfs:
        os.rename(target_path + pdf, '../PDFs/' + pdf)
    print('pdfs moved')
    # the above code should have made a directory ../PDFs for PDFs and moved them there.
    zips = [f for f in find_extension('.zip', path=target_path)]
    dirs = []
    for z in zips:
        d = dict(zip(zip_keys,z.split('.')))
        print('SITE = {} and PRNUM = {}'.format(d['SITE'], d['PRNUM']))
        dirs.append('{}{}/{}/'.format(target_path, d['SITE'], d['PRNUM'] ))
    zips = [target_path + z for z in zips]
    zf_dict = dict(zip(zips, dirs))
    unzipper(zf_dict)
    [os.remove(zip) for zip in zips]
    print('inner unzipped and deleted')
    # the above code should have unzipped the inner zips and removed them
    meta_path = target_path + 'META/'
    try:
        os.mkdir(meta_path)
    except FileExistsError:
        pass
    dirs = [d for d in os.listdir(target_path) if os.path.isdir(target_path + d)]
    for d in dirs:
        folders = [f for f in os.listdir(target_path + d) if os.path.isdir(target_path + d + '/' + f)]
        for f in folders:
            move_metadata(target_path + d + '/' + f +'/', meta_path)
    print('fin!')
    # the above code should have moved the metadata to META directory in target_path

def move_metadata(meta_source_path, meta_target_path):
    """ moves meta data file into directory specified by meta_target_path.
    In the future should be made to organise the meta data within meta_target_path"""
    txt = find_extension('.txt', path=meta_source_path)
    [os.rename(meta_source_path + f, meta_target_path + f) for f in txt]
    xml = find_extension('.xml', path=meta_source_path)
    [os.rename(meta_source_path + f, meta_target_path + f) for f in xml]
    csv = find_extension('.csv', path=meta_source_path)
    for f in csv:
        if 'sensor_positions' in f or 'variables' in f:
            os.rename(meta_source_path + f, meta_target_path + f)
    print('done moving metadata from {}'.format(meta_source_path))

'''
def purge():
    """ this wipes everything away and  restores the directory to the state found in ../NEON_recovery."""
    path = '../../../../../Thesis_Sandbox'
    rmtree(path)
    os.mkdir(path)
'''

def fix_1_minute(f):
    """ changes file with '_1min' in them to have '_1_minute' for consistency """
    if os.path.isdir(f) or f.endswith('.py'):
        pass
    else:
        v = f.split('.')
        if '_1min' in v[-5]:
            x = v[-5].partition('_1min')
            v[-5] = '_1_minute'.join((x[0],x[2]))
            new_f = '.'.join(v)
            os.rename(f,new_f)
        elif '_1_minute' in v[-5]:
            pass
        else:
            pass

def fix_30_minute(f):
    """ changes file with '_30min' in them to have '_30_minute' for consistency """
    if os.path.isdir(f) or f.endswith('.py'):
        pass
    else:
        v = f.split('.')
        if '_1min' in v[-5]:
            x = v[-5].partition('_30min')
            v[-5] = '_30_minute'.join((x[0],x[2]))
            new_f = '.'.join(v)
            os.rename(f,new_f)
        elif '_30_minute' in v[-5]:
            pass
        else:
            pass

def unzipper(zipFile_dir_dict, path=''):
    """ unzips the files that are the keys of the dict into the directories that
     are the values, which optionally can be located via path,
     then deletes the zip file """
    keys = list(zipFile_dir_dict.keys())
    for key in keys:
        print('unzipping {}\nto {}'.format(path + key, zipFile_dir_dict[key]))
        with zipfile.ZipFile(key, 'r') as f:
            if not os.path.isdir(path + zipFile_dir_dict[key]):
                print('not dir')
                os.makedirs(path + zipFile_dir_dict[key])
                f.extractall(path + zipFile_dir_dict[key])
            else:
                f.extractall(path + zipFile_dir_dict[key])
