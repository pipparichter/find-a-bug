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



def download_gtdb_gtdb_version(gtdb_version:int, ):
    '''Download GTDB data for the specified gtdb_version.'''
    url = f'https://data.gtdb.ecogenomic.org/gtdb_versions/gtdb_version{gtdb_version}/{gtdb_version}.0/'

    gtdb_version_dir = os.path.join(DATA_DIR, f'r{gtdb_version}')
    os.mkdir(gtdb_version_dir) # Make the directory for the new gtdb_version.
    os.mkdir(os.path.join(gtdb_version_dir, 'annotations'))
    os.mkdir(os.path.join(gtdb_version_dir, 'proteins'))
    os.mkdir(os.path.join(gtdb_version_dir, 'proteins', 'amino_acids'))
    os.mkdir(os.path.join(gtdb_version_dir, 'proteins', 'nucleotides'))
    os.mkdir(os.path.join(gtdb_version_dir, 'metadata'))

    # Download the metadata and sequences for representative genomes.  
    wget.download(url + f'bac120_metadata_r{gtdb_version}.tsv.gz', gtdb_version_dir)
    wget.download(url + f'ar53_metadata_r{gtdb_version}.tsv.gz', gtdb_version_dir)
    wget.download(url + f'genomic_files_reps/gtdb_proteins_aa_reps_r{gtdb_version}.tar.gz', gtdb_version_dir)
    wget.download(url + f'genomic_files_reps/gtdb_proteins_nt_reps_r{gtdb_version}.tar.gz', gtdb_version_dir)

    with tarfile.open(os.path.join(gtdb_version_dir, f'gtdb_proteins_aa_reps_r{gtdb_version}.tar.gz'), 'rb') as f:
        f.extractall(os.path.join(gtdb_version_dir, 'proteins', 'amino_acids'))
    with tarfile.open(os.path.join(gtdb_version_dir, f'gtdb_proteins_nt_reps_r{gtdb_version}.tar.gz'), 'rb') as f:
        f.extractall(os.path.join(gtdb_version_dir, 'proteins', 'nucleotides'))

    # Rename the files to not contain the '_protein' substring. 
    for root, dir_names, file_names in os.walk(os.path.join(gtdb_version_dir, 'proteins')):
        for file_name in file_names:
            if ('.fna' in file_name) or ('.faa' in file_name):
                source_path = os.path.join(root, file_name)
                dest_path = os.path.join(root, file_name.replace('_protein', ''))
                os.rename(source_path, dest_path)

    with gzip.open(os.path.join(gtdb_version_dir, f'ar53_metadata_r{gtdb_version}.tsv.gz'), 'rb') as f_source:
        with gzip.open(os.path.join(gtdb_version_dir, 'metadata', f'ar53_metadata_r{gtdb_version}.tsv.gz'), 'wb') as f_dest:
            shutil.copyfileobj(f_source, f_dest)

    with gzip.open(os.path.join(gtdb_version_dir, f'bac120_metadata_r{gtdb_version}.tsv.gz'), 'rb') as f_source:
        with gzip.open(os.path.join(gtdb_version_dir, 'metadata', f'bac120_metadata_r{gtdb_version}.tsv.gz'), 'wb') as f_dest:
            shutil.copyfileobj(f_source, f_dest)





def annotate_kegg(amino_acid_seqs_dir:int):
    pass


def annotate_pfam(amino_acid_seqs_dir:int):
    pass
