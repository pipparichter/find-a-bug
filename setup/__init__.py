import time
from utils import DATA_DIR
import wget
import glob 
import gzip 
import zipfile 
import tarfile
from tqdm import tqdm
from utils.database import Database
import shutil
import os 

# def download_gtdb(version:str):



def download_gtdb_release(release:int, ):
    '''Download GTDB data for the specified release.'''
    url = f'https://data.gtdb.ecogenomic.org/releases/release{release}/{release}.0/'

    release_dir = os.path.join(DATA_DIR, f'r{release}')
    os.mkdir(release_dir) # Make the directory for the new release.
    os.mkdir(os.path.join(release_dir, 'annotations'))
    os.mkdir(os.path.join(release_dir, 'amino_acid_seqs'))
    os.mkdir(os.path.join(release_dir, 'metadata'))

    # Download the metadata and sequences for representative genomes.  
    wget.download(url + f'bac120_metadata_r{release}.tsv.gz', release_dir)
    wget.download(url + f'ar53_metadata_r{release}.tsv.gz', release_dir)
    wget.download(url + f'genomic_files_reps/gtdb_proteins_aa_reps_r{release}.tar.gz', release_dir)

    with tarfile.open(os.path.join(release_dir, f'gtdb_proteins_aa_reps_r{release}.tar.gz'), 'rb') as f:
        f.extractall(os.path.join(release_dir, 'proteins'))
    # Rename all protein files to match convention...
    for file_name in os.listdir(os.path.join(release_dir, 'proteins', 'bacteria')):
        source_path = os.path.join(release_dir, 'proteins', 'bacteria', file_name)
        dest_path = os.path.join(release_dir, 'proteins', 'bacteria', file_name.replace('_protein', ''))
        os.rename(source_path, dest_path)
    for file_name in os.listdir(os.path.join(release_dir, 'proteins', 'archaea')):
        source_path = os.path.join(release_dir, 'proteins', 'bacteria', file_name)
        dest_path = os.path.join(release_dir, 'proteins', 'bacteria', file_name.replace('_protein', ''))
        os.rename(source_path, dest_path)

    with gzip.open(os.path.join(release_dir, f'ar53_metadata_r{release}.tsv.gz'), 'rb') as f_source:
        with gzip.open(os.path.join(release_dir, 'metadata', f'ar53_metadata_r{release}.tsv.gz'), 'wb') as f_dest:
            shutil.copyfileobj(f_source, f_dest)

    with gzip.open(os.path.join(release_dir, f'bac120_metadata_r{release}.tsv.gz'), 'rb') as f_source:
        with gzip.open(os.path.join(release_dir, 'metadata', f'bac120_metadata_r{release}.tsv.gz'), 'wb') as f_dest:
            shutil.copyfileobj(f_source, f_dest)





def annotate_kegg(amino_acid_seqs_dir:int):
    pass


def annotate_pfam(amino_acid_seqs_dir:int):
    pass
