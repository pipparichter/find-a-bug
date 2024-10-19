import argparse
import os 
import wget
import shutil
import urllib.request 
import urllib 
import glob
import warnings 
warnings.simplefilter('ignore') # Turn off annoying tarfile warnings

# I think the best way to store these big datasets is by zipping individual files and then 
# dumping them all into a tar archive. 


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', type=str, default='/var/lib/pgsql/data/gtdb/')
    parser.add_argument('--version', default=220, type=int)
    args = parser.parse_args()

    # Make the directory to store the new version of GTDB. 
    data_dir = os.path.join(args.data_dir, f'r{args.version}')
    os.makedirs(data_dir, exist_ok=True) # This should not delete anything!
    print(f'Created directory for storing the GTDB data at {data_dir}')

    # URL for the GTDB FTP site. 
    url = f'https://data.gtdb.ecogenomic.org/releases/release{args.version}/{args.version}.0/'

    # The files we need from GTDB for this are:
    #   'gtdb_proteins_nt_reps_r{version}.tar.gz'
    #   'gtdb_proteins_aa_reps_r{version}.tar.gz'
    #   'ar53_metadata_r{version}.tsv.gz
    #   'bac120_metadata_r{version}.tsv.gz

    file_name_map = dict()
    file_name_map[f'genomic_files_reps/gtdb_proteins_nt_reps_r{args.version}.tar.gz'] = f'proteins_nt.tar.gz'
    file_name_map[f'genomic_files_reps/gtdb_proteins_aa_reps_r{args.version}.tar.gz'] = f'proteins_aa.tar.gz'
    file_name_map[f'ar53_metadata_r{args.version}.tsv.gz'] = f'archaea_metadata.tsv.gz'
    file_name_map[f'ar53_metadata_r{args.version}.tar.gz'] = f'archaea_metadata.tar.gz'
    file_name_map[f'bac120_metadata_r{args.version}.tsv.gz'] = f'bacteria_metadata.tsv.gz'
    file_name_map[f'bac120_metadata_r{args.version}.tar.gz'] = f'bacteria_metadata.tar.gz'

    local_files = []
    for remote_file, local_file in file_name_map.items():

        # Skip downloading the file if it does not already exist. 
        if os.path.exists(os.path.join(data_dir, local_file)):
            continue

        try:
            print(f'Downloading file from {url + remote_file}')
            urllib.request.urlretrieve(url + remote_file, os.path.join(data_dir, local_file))
        except urllib.error.HTTPError:
            print(f'Failed to download file {remote_file}')

    # Only keep the files in the list which have been succesfully dowloaded. 
    local_files = [file for file in local_files if os.path.exists(os.path.join(data_dir, file))]
    assert len(local_files) == 4, f'There should only be 4 files in the local_files list. Found {len(local_files)}.'
        
    # # First, make all necessary directories... 
    # os.makedirs(os.path.join(data_dir, 'proteins', 'nucleotides'), exist_ok=True)
    # os.makedirs(os.path.join(data_dir, 'proteins', 'amino_acids'), exist_ok=True)
    # os.makedirs(os.path.join(data_dir, 'metadata'), exist_ok=True)
    
    # extract(os.path.join(data_dir, local_files[0]), dst_path=os.path.join(data_dir, 'proteins', 'nucleotides'))
    # extract(os.path.join(data_dir, local_files[1]), dst_path=os.path.join(data_dir, 'proteins', 'amino_acids'))
    # extract(os.path.join(data_dir, local_files[2]), dst_path=os.path.join(data_dir, 'metadata'))
    # extract(os.path.join(data_dir, local_files[3]), dst_path=os.path.join(data_dir, 'metadata'))

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
