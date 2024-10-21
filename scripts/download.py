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
warnings.simplefilter('ignore') # Turn off annoying tarfile warnings

# I think the best way to store these big datasets is by zipping individual files and then 
# dumping them all into a tar archive. 


# Unfortunately, in order to unpack single member of .tar.gz archive you have to process whole archive, and not much you can do to fix it.
# https://superuser.com/questions/655739/extract-single-file-from-huge-tgz-file 
# Seems like it might make more sense to store as a Zipfile, or multiple zipped files in an unzipped directory. 
# (this does not seem to take more memory, see https://superuser.com/questions/908193/is-it-better-to-compress-all-data-or-compressed-directories)

def unpack(archive_path:str, remove:bool=False):
    '''Convert a tar.gz file into a direcroty of compressed files to make parallelizing upload easier. This should not take
    more memory than zipping the entire tar archive (which I confirmed by testing locally).'''
    print(f'unpack: Unpacking tar archive at {archive_path}')
    dir_path = os.path.dirname(archive_path)
    dir_path = os.path.join(dir_path, os.path.basename(archive_path).split('.')[0]) # Get the archive name and remove extensions. 
    os.makedirs(dir_path, exist_ok=True) # Make the new directory. 

    def write(contents:str, path:str):
        '''Write the contents to a zipped file at the specified path. contents should be a binary string.
        If the path is already'''

        with gzip.open(path, 'wb') as f:
            f.write(contents)
    
    with tarfile.open(archive_path, 'r:gz') as archive:
        for member in tqdm(archive.getmembers(), desc=f'unpack: Unpacking archive {archive_path}...'):
            if member.isfile():
                contents = archive.extractfile(member).read()
                file_name = os.path.basename(member.name) # + '.gz'
                assert '.gz' in file_name, f'unpack: Expected a zipped file in the tar archive, but found {file_name}.'
                write(contents, os.path.join(dir_path, file_name))
    
    if remove: # Remove the original archive if specified. 
        os.remove(archive_path)


def unpack_metadata(metadata_file_path:str, remove:bool=False):

    dir_path = os.path.dirname(metadata_file_path)
    # Get the name of the output file... 
    output_file_name = os.path.basename(metadata_file_path).split('.')[0] + '.tsv'
    output_path = os.path.join(dir_path, output_file_name)

    def write(contents:str, path:str):
        '''Write the contents to a zipped file at the specified path. contents should be a binary string.'''
        with open(path, 'wb') as f:
            f.write(contents)

    if not os.path.exists(output_path): # Only proceed if the file has not already been created. 
        # Metadata files can be stored as tar objects or as regular zipped TSV files, depending on the GTDB version.
        # if tarfile.is_tarfile(metadata_file_path):
        if ('.tar' in metadata_file_path):
            with tarfile.open(archive_path, 'r:gz') as archive: 
                members = archive.getmembers()
                assert len(members) == 1, f'unpack_metadata: There should only be 1 item in the metadata archive. Found (len(members)).'
                write(archive.extractfile(member).read(), output_path)
        elif ('.gz' in metadata_file_path):
            with gzip.open(metadata_file_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        print(f'unpack_metadata: Metadata unzipped and written to {output_path}')
    
    if remove: # Remove original file if specified. 
        os.remove(metadata_file_path)


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
        unpack_metadata(metadata_file_path)

    # archive_paths = [os.path.join(data_dir, path) for path in os.listdir(data_dir) if (tarfile.is_tarfile(path) and (path not in metadata_file_paths))]
    archive_paths = [os.path.join(data_dir, path) for path in os.listdir(data_dir) if (('.tar' in path) and (path not in metadata_file_paths))]
    # assert len(archive_paths) == 4, f'There should only be 4 tar archives in the data directory. Found {len(archive_paths)}.'
    for archive_path in archive_paths:
        unpack(archive_path, remove=False)
        
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
