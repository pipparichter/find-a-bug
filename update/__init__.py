import time
from utils import DATA_DIR
import wget
import glob 
import gzip 
import zipfile 
import tarfile
import sqlalchemy 
import shutil
import os 

# def download_gtdb(version:str):

# First want to move all info in current tables to the history tables. 
def move_to_history(engine:sqlalchemy.engine.Engine, table_name:str):
    '''Move a current table to history.'''

    history_table_name = table_name + '_history'
    with engine.begin() as conn: # Open a connection to the database. 
        conn.execute(f'INSERT INTO {history_table_name} SELECT * FROM {table_name}')
        # Delete all rows from the current table. Not specifying "WHERE" removes everything.
        conn.execute(f'DELETE FROM {table_name}')


def download_gtdb_release(release:int, data_dir:str=DATA_DIR):

    url = f'https://data.gtdb.ecogenomic.org/releases/release{release}/{release}.0/'

    release_dir = os.path.join(data_dir, f'r{release}')
    os.mkdir(release_dir) # Make the directory for the new release.
    os.mkdir(os.path.join(release_dir, 'annotations'))
    os.mkdir(os.path.join(release_dir, 'amino_acid_seqs'))
    os.mkdir(os.path.join(release_dir, 'metadata'))

    # Download the metadata and sequences for representative genomes.  
    wget.download(url + f'bac120_metadata_r{release}.tsv.gz', release_dir)
    wget.download(url + f'ar53_metadata_r{release}.tsv.gz', release_dir)
    wget.download(url + f'genomic_files_reps/gtdb_proteins_aa_reps_r{release}.tar.gz', release_dir)

    with tarfile.open(os.path.join(release_dir, f'gtdb_proteins_aa_reps_r{release}.tar.gz'), 'rb') as f:
        f.extractall(os.path.join(release_dir, 'amino_acid_seqs'))

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