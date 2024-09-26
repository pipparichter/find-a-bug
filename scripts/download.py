import argparse
import os 
import wget
import gzip 
import tarfile 
import shutil
import urllib.request 

# Structure of the GTDB version directories is as follows:
# /var/lib/pgsql/data/gtdb/r{version}/
#   ./annotations/pfam
#   ./annotations/kegg
#   ./proteins/amino_acids/
#       ./bacteria
#       ./archaea
#   ./proteins/nucleotides/
#       ./bacteria
#       ./archaea
#   ./metadata/

def extract_tar(tar_path:str=None, dst_path:str=None, src_path:str=None): 
    '''
    :param tar_path: The path to the tar archive to extract. 
    :param dst_path: The path where all the contents of the tar archive will be written. 
    :param src_path: The name of the directory where the extracted tar archive is stored. I needed to set
        this manually. 
    '''
    print(f'extract_tar: Extracting tar archive {tar_path}')

    with tarfile.open(tar_path, 'r') as tar:
        tar.extractall(path=os.path.dirname(tar_path))
    # src_path is the path to the newly-extracted directory. I can't come up with a good way to automatically
    # detect what it's name is, so just set it manually. 
    print(f'extract_tar: Moving files from {src_path} to {dst_path}.')
    for root, dirs, files in os.walk(src_filepath):   
        for file in files:     
            shutil.move(os.path.join(root, file), os.path.join(dst_path, file))

    shutil.rmtree(src_path) # Remove the src_path directory, which should be empty now.


def extract_gz(gz_path:str=None, dst_path:str=None): 
    print(f'extract_gz: Extracting gz file {gz_path}')
    with gzip.open(gz_path, 'rb') as f:
        contents = f.read()
    with open(dst_path, 'w') as f:
        f.write(contents)
 


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', type=str, default='/var/lib/pgsql/data/gtdb/')
    parser.add_argument('--version', default=220, type=int)
    args = parser.parse_args()

    # Make the directory to store the new version of GTDB. 
    data_dir = os.path.join(args.data_dir, f'r{args.version}')
    os.makedirs(data_dir, exist_ok=True)
    print(f'Created directory for storing the GTDB data at {data_dir}')

    # URL for the GTDB FTP site. 
    url = f'https://data.gtdb.ecogenomic.org/releases/release{args.version}/{args.version}.0/'

    # The files we need from GTDB for this are:
    #   'gtdb_proteins_nt_reps_r{version}.tar.gz'
    #   'gtdb_proteins_aa_reps_r{version}.tar.gz'
    #   'ar53_metadata_r{version}.tsv.gz
    #   'bac120_metadata_r{version}.tsv.gz
    remote_files = [f'genomic_files_reps/gtdb_proteins_nt_reps_r{args.version}.tar.gz']
    remote_files += [f'genomic_files_reps/gtdb_proteins_aa_reps_r{args.version}.tar.gz']
    remote_files += [f'ar53_metadata_r{args.version}.tsv.gz']
    remote_files += [f'bac120_metadata_r{args.version}.tsv.gz']

    local_files = []
    for remote_file in remote_files:
        local_file = remote_file.split('/')[-1]
        local_files.append(local_file)
        # Download the file if it does not already exist. 
        if not os.path.exists(os.path.join(data_dir, local_file)):
            print(f'Downloading file from {url + remote_file}')
            urllib.request.urlretrieve(rl + remote_file, os.path.join(data_dir, local_file))
            # local_files.append(wget.download(url + remote_file, out=data_dir))
            urllib.request.urlretrieve(rl + remote_file, os.path.join(data_dir, local_file))
    
    # First, make all necessary directories... 
    os.makedirs(os.path.join(data_dir, 'proteins', 'nucleotides'), exist_ok=True)
    os.makedirs(os.path.join(data_dir, 'proteins', 'amino_acids'), exist_ok=True)
    os.makedirs(os.path.join(data_dir, 'metadata'), exist_ok=True)
    
    extract_tar(tar_path=os.path.join(data_dir, local_files[0]), dst_path=os.path.join(data_dir, 'proteins', 'nucleotides'), src_path=os.path.join(data_dir, 'protein_fna_reps'))
    extract_tar(tar_path=os.path.join(data_dir, local_files[1]), dst_path=os.path.join(data_dir, 'proteins', 'amino_acids'), src_path=os.path.join(data_dir, 'protein_faa_reps'))
    extract_gz(gz_path=os.path.join(data_dir, local_files[2]), dst_path=os.path.join(data_dir, 'metadata', local_files[2].replace('.gz', '')))
    extract_gz(gz_path=os.path.join(data_dir, local_files[3]), dst_path=os.path.join(data_dir, 'metadata', local_files[3].replace('.gz', '')))






