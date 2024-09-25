import argparse
import os 
import wget
import gzip 
import tarfile 
import shutil

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

def extract(tar_path:str, dst_path:str): 
    print(f'extract: Extracting file {tar_path}')
    with tarfile.open(tar_path, 'rb') as tar:
        tar.extractall(path=data_dir)
    # src_path is the path to the newly-extracted directory. 
    src_path = os.path.join(data_dir, tar_path.replace('.tar.gz', ''))
    for item in os.listdir(new_path):
        shutil.move(os.path.join(src_path, item), os.path.join(dst_path, item))
    shutil.rmtree(src_path) # Remove the src_path directory, which should be empty now. 


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
    for file in files:
        print(f'Downloading file from {url + file}')
        local_files.append(wget.download(url + file, out=data_dir))
    
    # First, make all necessary directories... 
    os.makedirs(os.path.join(data_dir, 'proteins', 'nucleotides'), exist_ok=True)
    os.makedirs(os.path.join(data_dir, 'proteins', 'amino_acids'), exist_ok=True)
    os.makedirs(os.path.join(data_dir, 'metadata'), exist_ok=True)
    
    extract(os.path.join(data_dir, local_files[0]), os.path.join(data_dir, 'proteins', 'nucleotides'))
    extract(os.path.join(data_dir, local_files[1]), os.path.join(data_dir, 'proteins', 'amino_acids'))
    extract(os.path.join(data_dir, local_files[2]), os.path.join(data_dir, 'metadata'))






