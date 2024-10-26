'''Script for initializing the Find-A-Bug database using the data already present on the microbes.gps.caltech.edu server.'''
import os
os.sys.path.append('../utils/')
import argparse
from utils.database import Database
from utils.files import * 
from tqdm import tqdm
import zipfile 
import glob
import tarfile
from typing import List, Tuple
from multiprocess import Pool

DATA_DIR = '/var/lib/pgsql/data/gtdb/'
N_WORKERS = 10

# In an attempt to speed this up, going to try to parallelize the decompression step of the process. 
# I can either do this by parallizing the upload_files function, or chunk processing within the upload_files function. 

# TODO: Verify that the rate-limiting step is actually decompression, and not the upload to the database. 
# TODO: What is the maximum chunk I can read into RAM? Then I can avoid the overhead of writing the extracted ZIP files to separate files. 


def upload(paths:List[str], table_name:str, file_class:File, pbar):
    '''Upload a chunk of zipped files to the Find-A-Bug database. .

    :param paths:
    :param database: The Database object which connects to the Find-A-Bug database. 
    :param table_name: The name of the table in the database where the data will be uploaded. 
    :param file_class: The type of file being uploaded to the database. 
    '''
    entries = []
    for path in paths:
        file = file_class(path, version=VERSION)
        entries += file.entries()
    DATABASE.bulk_upload(table_name, entries)
    if pbar is not None:
        pbar.update(len(entries))


def upload_proteins(paths:List[Tuple[str, str]], table_name:str, file_class:ProteinsFile, pbar):
    '''A function for handling upload of protein sequence files to the database, which is necessary because separate 
    nucleotide and amino acid files need to be combined in a single upload to the proteins table.
    
    :param paths
    :param database: The Database object which connects to the Find-A-Bug database. 
    '''
    entries = []
    for aa_path, nt_path in paths:
        nt_file, aa_file = ProteinsFile(nt_path, version=VERSION), ProteinsFile(aa_path, version=VERSION)
        assert aa_file.size() == nt_file.size(), 'upload_proteins_files: The number of entries in corresponding nucleotide and amino acid files should match.' 
            
        for aa_entry, nt_entry in zip(aa_file.entries(), nt_file.entries()):
            assert aa_entry['gene_id'] == nt_entry['gene_id'], 'upload_proteins_files: Gene IDs in corresponding amino acid and nucleotide files should match.'  
            entry = aa_entry.copy() # Merge the nucleotide and amino acid entries. 
            entry.update({f:v for f, v in nt_entry.items()}) # Nucleotide sequences don't fit in table.
            entries.append(entry)

    DATABASE.bulk_upload(table_name, entries) 
    if pbar is not None:
        pbar.update(len(entries))


def parallelize(paths:List[str], upload_func, table_name:str, file_class:File, chunk_size:int=500):

    pbar = tqdm(desc=f'parallelize: Uploading to table {table_name}...', total=len(paths)) 

    chunks = [paths[i * chunk_size: (i + 1) * chunk_size] for i in range(len(paths) // chunk_size + 1)]
    args = [(chunk, table_name, file_class, pbar) for chunk in chunks]

    # TODO: Read more about how this works. 
    # https://stackoverflow.com/questions/53751050/multiprocessing-understanding-logic-behind-chunksize 
    # TODO: Read about starmap versus map. Need this for iterable arguments. 
    pool = Pool(os.cpu_count()) # I think this should manage the queue for me. 
    for _ in tqdm(pool.starmap(upload_func, args), desc=f'parallelize: Uploading to table {table_name}.', total=len(args)):
        pass
    
    pbar.close()
    pool.close()



if __name__ == '__main__':
    
    global DATABASE # Need to declare as global for multiprocessing to work. 
    DATABASE = Database(reflect=False)

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=207, type=int, help='The GTDB version to upload to the SQL database.')
    parser.add_argument('--drop-existing', action='store_true')
    args = parser.parse_args()

    global VERSION # Just set the global parameter to reduce argument number. 
    VERSION = args.version 
    
    data_dir = os.path.join(DATA_DIR, f'r{VERSION}')

    if args.drop_existing:
        for table_name in DATABASE.table_names[::-1]:
            print(f'Dropping existing table {table_name}.')
            DATABASE.drop(table_name)

    for table_name in DATABASE.table_names:
        print(f'Initializing table {table_name}.')
        DATABASE.create(table_name)

    DATABASE.reflect()

    # NOTE: Table uploads must be done sequentially, i.e. the entire metadata table needs to be up before anything else. 

    print(f'Uploading data to the metadata_r{VERSION} table.')
    metadata_paths = glob.glob(os.path.join(data_dir, '*metadata*.tsv')) # This should output the full paths. 
    # upload(metadata_paths, database, f'metadata_r{VERSION}', MetadataFile)
    upload(metadata_paths, f'metadata_r{VERSION}', MetadataFile, None)

    # Need to upload amino acid and nucleotide data simultaneously.
    proteins_aa_dir, proteins_nt_dir = os.path.join(data_dir, 'proteins_aa'), os.path.join(data_dir, 'proteins_nt')
    proteins_aa_paths = [os.path.join(proteins_aa_dir, file_name) for file_name in os.listdir(proteins_aa_dir) if (file_name != 'gtdb_release_tk.log.gz')]
    proteins_nt_paths = [os.path.join(proteins_nt_dir, file_name) for file_name in os.listdir(proteins_nt_dir)]
    paths = [(aa_path, nt_path) for aa_path, nt_path in zip(sorted(proteins_aa_paths), sorted(proteins_nt_paths))]
    # parallelize(paths, upload_proteins, database, f'proteins_r{VERSION}', ProteinsFile)
    parallelize(paths, upload_proteins, f'proteins_r{VERSION}', ProteinsFile)


    annotations_kegg_dir = os.path.join(data_dir, 'annotations_kegg')
    paths = [os.path.join(annotations_kegg_dir, file_name) for file_name in os.listdir(annotations_kegg_dir)]
    # parallelize(path, upload, database, f'annotations_kegg_r{VERSION}', KeggAnnotationsFile)
    parallelize(path, upload, f'annotations_kegg_r{VERSION}', KeggAnnotationsFile)

    DATABASE.close()
    