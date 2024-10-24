import argparse
import os 
import wget
import shutil
import urllib.request 
import urllib 
import glob
import gzip
import warnings 
from tqdm import tqdm
import tarfile
import numpy as np 
warnings.simplefilter('ignore') # Turn off annoying tarfile warnings
from time import perf_counter, sleep
import threading  
from typing import List
from queue import Queue
import subprocess

# NOTE: Why is the tar file like 10000 times the size of the original annotation file?

# Unfortunately, in order to unpack single member of .tar.gz archive you have to process whole archive, and not much you can do to fix it.
# https://superuser.com/questions/655739/extract-single-file-from-huge-tgz-file 
# Seems like it might make more sense to store as a Zipfile, or multiple zipped files in an unzipped directory. 
# (this does not seem to take more memory, see https://superuser.com/questions/908193/is-it-better-to-compress-all-data-or-compressed-directories)


# N_WORKERS = 10 

def time(func, *args):
    t1 = perf_counter()
    output = func(*args)
    t2 = perf_counter()
    print(f'time: Function {func.__name__} completed in {np.round(t2 - t1, 2)} seconds.')
    return output

def check(output_paths:List[str]):
    pbar = tqdm(output_paths, desc=f'check: Checking extracted files...')
    for path in pbar:
        pbar.update(1)
        pbar.set_description(f'check: Checking extracted files... {path}')
        assert os.path.extracted(path), f'check: It seems as though the file {path} does not exist.'
        with gzip.open(path, 'r') as f:
            content = f.read()
            # assert len(content) > 0, f'check: It seems as though the gzip-compressed file {path} is empty.'
        # try:
        #     with gzip.open(path, 'r') as f:
        #         content = f.read().decode()
        #         assert len(content) > 0, f'check: It seems as though the gzip-compressed file {path} is empty.'
        # except:
        #     # raise Exception(f'check: There was a problem reading the gzip-compressed file {path}.')
        #     print(f'check: There was a problem reading the gzip-compressed file {path}.')



def processed(path:str, output_dir:str) -> bool:
    '''Takes a list of members from the tar archive and checks to see if they are already present in the
    output directory.'''
    file_name = os.path.basename(path) # Remove the relative path from the member name. 
    file_name = add_gz(file_name) # File name will contain the zip extension in the output directory. 
    return os.path.exists(os.path.join(output_dir, file_name))


def add_gz(file_name:str) -> str:
    '''Add the gz extension to the file name if it is not already there.'''
    file_name + '.gz' if ('.gz' not in file_name) else file_name
    return file_name

def remove_gz(file_name:str) -> str:
    '''Remove the gz extension from the file name.'''
    file_name = file_name.replace('.gz', '')
    return file_name

def compressed(file_name:str) -> str:
    '''Check if a file is compressed, i.e. if it has the .gz file extension.'''
    return '.gz' in file_name


# NOTE: Took about a half hour to extract... 
def extract(archive_path:str):
    '''Extract the tar archive to a temporary directory.'''
    output_dir = archive_path.replace('.tar.gz', '.tmp')
    if not os.path.exists(output_dir): # Don't try to re-extract if it exists. 
        os.makedirs(output_dir, exist_ok=True)
        subprocess.run(f'tar -xf {archive_path} -C {output_dir} --strip=1', shell=True, check=True)
    return output_dir


def process(path:str, output_dir:str, pbar=None):
    '''Extract the a file from a tar archive and plop it at the specified path. There are several cases: (1) the file contained
    in the tar archive is already zipped and just needs to be moved and (2) the file is not zipped and needs to be compressed.'''
    output_path = os.path.join(output_dir, os.path.basename(add_gz(path)))
    if compressed(path): # If the file is already compressed, don't try to re-compress it. 
        shutil.move(path, output_path)
    else:
        with open(path, 'rb') as f_in, gzip.open(output_path, 'wb') as f_out:
            f_out.write(f_in.read())
    return output_path


def unpack(archive_path:str, remove:bool=False):
    '''Convert a tar.gz file into a direcroty of compressed files to make parallelizing upload easier. This should not take
    more memory than zipping the entire tar archive (which I confirmed by testing locally).'''
    print(f'unpack: Unpacking tar archive at {archive_path}')
    output_dir = os.path.dirname(archive_path)
    output_dir = os.path.join(output_dir, os.path.basename(archive_path).split('.')[0]) # Get the archive name and remove extensions. 
    os.makedirs(output_dir, exist_ok=True) # Make the new directory. 
    print(f'unpack: Created output directory at {output_dir}')

    extracted_archive_path = extract(archive_path)

    output_paths = []
    input_paths = []
    for root, dirs, files in os.walk(extracted_archive_path):
        for file in files:
            input_path = os.path.join(root, file)
            if not processed(input_path, output_dir):
                input_paths.append(input_path)

    for input_path in tqdm(input_paths, f'unpack: Unpacking extracted tar archive {extracted_archive_path}'):
        output_path = process(input_path, output_dir)
        output_paths.append(output_path)

    if remove: # Remove the original archive if specified. 
        os.remove(archive_path)
        os.remove(extracted_archive_path)

    return output_paths



def unpack_metadata(metadata_file_path:str, remove:bool=False):
    '''Metadata files can either be tar archives or simple zipped TSV files, depending on the 
    version of GTDB. This function just gets the thing out of the tar archive cormat, if that's 
    what it's in (I wonder why they did this?)'''
    output_dir = os.path.dirname(metadata_file_path)
    # Get the name of the output file... 
    output_file_name = os.path.basename(metadata_file_path).split('.')[0] + '.tsv'
    output_path = os.path.join(output_dir, output_file_name)

    if not os.path.exists(output_path): # Only proceed if the file has not already been created. 
        # Metadata files can be stored as tar objects or as regular zipped TSV files, depending on the GTDB version.
        # if tarfile.is_tarfile(metadata_file_path):
        if ('.tar' in metadata_file_path):
            archive = tarfile.open(metadata_file_path, 'r:gz')
            member = [m for m in archive.getmembers() if m.isfile()][0]
            member.name = output_file_name
            archive.extract(member, output_dir)
            archive.close()

        print(f'unpack_metadata: Metadata unzipped and written to {output_path}')
    
    if remove: # Remove original file if specified. 
        os.remove(metadata_file_path)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', type=str, default='/var/lib/pgsql/data/gtdb/')
    parser.add_argument('--version', default=207, type=int)
    # parser.add_argument('--multithread', action='store_true')
    # parser.add_argument('--n-workers', type=int, default=4)
    args = parser.parse_args()

    # test_archive_path = '/var/lib/pgsql/data/gtdb/r207/test.tar.gz'
    # if args.multithread:
    #     output_paths = time(unpack_multithread, test_archive_path, args.n_workers)
    # else:
    #     output_paths = time(unpack, test_archive_path)
    # check(output_paths)
    # shutil.rmtree('/var/lib/pgsql/data/gtdb/r207/test')
    # exit(1)

    # Make the directory to store the new version of GTDB. 
    data_dir = os.path.join(args.data_dir, f'r{args.version}')
    os.makedirs(data_dir, exist_ok=True) # This should not delete anything!
    print(f'Created directory for storing the GTDB data at {data_dir}')

    # URL for the GTDB FTP site. 
    url = f'https://data.gtdb.ecogenomic.org/releases/release{args.version}/{args.version}.0/'

    file_name_map = dict()
    file_name_map[f'genomic_files_reps/gtdb_proteins_nt_reps_r{args.version}.tar.gz'] = f'proteins_nt.tar.gz'
    file_name_map[f'genomic_files_reps/gtdb_proteins_aa_reps_r{args.version}.tar.gz'] = f'proteins_aa.tar.gz'
    file_name_map[f'ar53_metadata_r{args.version}.tsv.gz'] = f'archaea_metadata.tsv.gz'
    file_name_map[f'ar53_metadata_r{args.version}.tar.gz'] = f'archaea_metadata.tar.gz'
    file_name_map[f'bac120_metadata_r{args.version}.tsv.gz'] = f'bacteria_metadata.tsv.gz'
    file_name_map[f'bac120_metadata_r{args.version}.tar.gz'] = f'bacteria_metadata.tar.gz'

    for remote_file, local_file in file_name_map.items():

        # Skip downloading the file if it does not already exist. 
        if os.path.exists(os.path.join(data_dir, local_file)):
            continue
        try:
            print(f'Downloading file from {url + remote_file}')
            urllib.request.urlretrieve(url + remote_file, os.path.join(data_dir, local_file))
        except urllib.error.HTTPError:
            print(f'Failed to download file {remote_file}')

    # Need to handle the metadata files differently... 
    metadata_file_paths = [os.path.join(data_dir, file_name) for file_name in os.listdir(data_dir) if ('metadata' in file_name)]
    for metadata_file_path in metadata_file_paths:
        unpack_metadata(metadata_file_path, remove=False)

    # archive_paths = [os.path.join(data_dir, path) for path in os.listdir(data_dir) if (tarfile.is_tarfile(path) and (path not in metadata_file_paths))]
    archive_paths = [os.path.join(data_dir, file_name) for file_name in os.listdir(data_dir) if (('.tar' in file_name) and ('metadata' not in file_name))]
    for archive_path in sorted(archive_paths):
        unpack(archive_path, remove=False)
       
# def get_latest(dir_path:str) -> str:
#     '''Get the most recently-created file in the specified directory. Returns a complete path, 
#     not just the filename.'''
#     items = glob.glob(os.path.join(dir_path, '*')) # Get all files and subdirectories in the directory. 
#     latest_item = max(items, key=os.path.getctime)
#     return os.path.join(dir_path, latest_item)


# def extract_tar(tar_path:str=None, dst_path:str=None): # , src_path:str=None): 
#     '''Extract a zipped tar file to the specified directory. 

#     :param tar_path: The path to the tar archive to extract. 
#     :param dst_path: The path where all the contents of the tar archive will be written. 
#     '''
#     print(f'extract_tar: Extracting tar archive {tar_path}')

#     with tarfile.open(tar_path, 'r') as tar:
#         # Extract the tar archive into the same directory. 
#         tar.extractall(path=os.path.dirname(tar_path))

#     # src_path is the path to the newly-extracted file or directory. This should be the most-recently created item in the directory. 
#     src_path = get_latest(os.path.dirname(tar_path))

#     if os.path.isdir(src_path):
#         print(f'extract_tar: Moving files from {src_path} to {dst_path}.')
#         for root, dirs, files in os.walk(src_path):   
#             for file in files:     
#                 shutil.move(os.path.join(root, file), os.path.join(dst_path, file))
#                 # All the files in the tar archive are gzipped. Need to extract these as well.  
#                 if file.split('.')[-1] == 'gz':
#                     extract_gz(os.path.join(dst_path, file), dst_path=dst_path, verbose=False)
#         shutil.rmtree(src_path) # Remove the src_path directory, which should be empty now.

#     else: # If the extracted item is not a directory, just move it to the destination path. 
#         print(f'extract_tar: Moving file from {src_path} to {dst_path}.')
#         shutil.move(src_path, os.path.join(dst_path, os.path.basename(src_path)))


# def extract_gz(gz_path:str=None, dst_path:str=None, verbose:bool=True): # , rm:bool=True, 
#     '''Extract a zipped file to the specified directory.
    
#     :param gz_path: The path to the zipped file. 
#     :param dst_path: The path where to which file will be extracted. 
#     :param verbose: Whether or not to print a little update. 
#     '''
#     if verbose: print(f'extract_gz: Extracting gz file {gz_path}')

#     assert os.path.isdir(dst_path), 'extract_gz: dst_path must be a directory.'
#     # Use the filename without the gz extension as the destination file. 
#     dst_path = os.path.join(dst_path, gz_path.replace('.gz', '')) 

#     with gzip.open(gz_path, 'rb') as src:
#         with open(dst_path, 'wb') as dst:
#             shutil.copyfileobj(src, dst)
 

# def extract(path:str, dst_path:str=None):
#     '''Extract a compressed file, either tar or gz.'''
#     # tar takes precedence over gz. This is because for the non-directories with the .tar.gz file extension, 
#     # the tar utility fully decompresses the file, while gunzip does not.
#     if '.tar' in path:
#         extract_tar(tar_path=path, dst_path=dst_path)
#     elif '.gz' in path:
#         extract_gz(gz_path=path, dst_path=dst_path)
#     else:
#         raise Exception(f'extract: Compressed file at {path} cannot be extracted.')
