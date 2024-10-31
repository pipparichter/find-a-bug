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
from typing import List, Tuple, Dict
from multiprocess import Pool, Value, Lock
import sys 
import numpy as np 
import time 
import datetime 
import pymysql

DATA_DIR = '/var/lib/pgsql/data/gtdb/'
CHUNK_SIZE = 100

def timestamp() -> str:
    now = datetime.datetime.now()
    return now.strftime('%Y-%m-%d_%H:%M:%S')


# NOTE: There are 200505361 proteins across all files in the source directory, and only 
# 200486859 in the database (missing 18502). For some reason, a couple thousand proteins aren't getting uploaded, 
# and it doesn't seem like a problem with the file class. 
# Maybe some of the protein IDs are the same? No, behavior of insert means that this should throw an error. 
# All of the genomes are represented though, so it is somewhere else in the upload step. 

# In an attempt to speed this up, going to try to parallelize the decompression step of the process. 
# I can either do this by parallizing the upload_files function, or chunk processing within the upload_files function. 

# TODO: Verify that the rate-limiting step is actually decompression, and not the upload to the database. 
# TODO: What is the maximum chunk I can read into RAM? Then I can avoid the overhead of writing the extracted ZIP files to separate files. 

class Counter():
    '''A process-safe counter, as described here: https://superfastpython.com/process-safe-counter/'''
    def __init__(self, total:int=None):
        self._counter = Value('i', 0)
        self._time = Value('d', 0)
        self._lock = Lock()
        self.total = total
        # NOTE: Read about locks here: https://superfastpython.com/multiprocessing-mutex-lock-in-python/
        # NOTE: Read about shared Ctypes here: https://superfastpython.com/multiprocessing-shared-ctypes-in-python/

    def update(self, n:int, t:float=0):
        with self._lock:
            self._counter.value += n
            self._time.value += t

    def value(self) -> int:
        return self._counter.value
    
    def __str__(self) -> str:
        return str(self.value())

    def __repr__(self) -> str:
        return str(self.value())
    
    def print(self):
        # Clear the previous line if this is not the first call to counter. 
        with self._lock: # Make sure multiple processes don't call this at the same time. 
            if self.value() > 0:
                sys.stdout.write('\r')
                sys.stdout.flush()
            print(f'Counter.show: {str(self)} out of {self.total}. Elapsed time is {np.round(self._time.value)} seconds.', end='\r')


def error_callback(error):
    print(f'\n{error}')


def show_progress(n:int, t:float=0):
    global COUNTER
    if COUNTER is not None:
        COUNTER.update(n, t=t)
        COUNTER.print()


def handle_upload_error(err, entries:List[Dict], table_name:str):
    '''When a bulk upload fails, Upload the entries one-by-one, and log the ones which throw an error.'''
    failed_entries = []
    for entry in entries:
        try:
            DATABASE.upload(table_name, entry)
        except:
            failed_entries.append(entry)
    df = pd.DataFrame(failed_entries) # Convert the entries to a DataFrame. 

    log_path = os.path.join(os.getcwd(), 'log', f'upload_failure_{table_name}_{timestamp()}.csv')
    log = open(log_path, 'w')
    # Add a comment marker to each line of the error message and write it to the file. 
    err = '\n'.join(['# ' + line for line in str(err).split('\n')])
    log.write(f'{err}\n')
    df.to_csv(log, index=False)
    log.close()


def upload(paths:List[str], table_name:str, file_class:File):
    '''Upload a chunk of zipped files to the Find-A-Bug database. .

    :param paths:
    :param table_name: The name of the table in the database where the data will be uploaded. 
    :param file_class: The type of file being uploaded to the database. 
    '''
    t_start = time.perf_counter()

    entries, failed_entries = [], []
    for path in paths:
        file = file_class(path, version=VERSION)
        entries += file.entries()
    try:
        DATABASE.bulk_upload(table_name, entries)
    except pymysql.err.IntegrityError as err:
        # In case of an exception, switch to uploading one at a time to figure out where the problem is. 
        failed_entries = handle_upload_error(err, failed_entries, table_name)
    
    t_finish = time.perf_counter()
    show_progress(len(paths), t=t_finish - t_start)

    return len(entries) - len(failed_entries)
    


def upload_proteins(paths:List[Tuple[str, str]], table_name:str, file_class:ProteinsFile):
    '''A function for handling upload of protein sequence files to the database, which is necessary because separate 
    nucleotide and amino acid files need to be combined in a single upload to the proteins table.
    
    '''
    t_start = time.perf_counter()
    entries, failed_entries = [], []

    for aa_path, nt_path in paths:
        nt_file, aa_file = ProteinsFile(nt_path, version=VERSION), ProteinsFile(aa_path, version=VERSION)
        assert aa_file.size() == nt_file.size(), 'upload_proteins_files: The number of entries in corresponding nucleotide and amino acid files should match.' 
        for aa_entry, nt_entry in zip(aa_file.entries(), nt_file.entries()):
            assert aa_entry['gene_id'] == nt_entry['gene_id'], 'upload_proteins_files: Gene IDs in corresponding amino acid and nucleotide files should match.'  
            entry = aa_entry.copy() # Merge the nucleotide and amino acid entries. 
            entry.update({f:v for f, v in nt_entry.items()}) # Nucleotide sequences don't fit in table.
            entries.append(entry)
    try:
        # assert len(entries) == total, f'upload_proteins_files: Expected {total} entries, but saw {len(entries)}.'
        DATABASE.bulk_upload(table_name, entries)
    except pymysql.err.IntegrityError as err: # In case of upload failure, write the failed upload to a CSV file. 
        failed_entries = handle_upload_error(err, entries, table_name)
        
    t_finish = time.perf_counter()
    show_progress(len(paths), t=t_finish - t_start)

    return len(entries) - len(failed_entries)


def parallelize(paths:List[str], upload_func, table_name:str, file_class:File, chunk_size:int=100):

    # reset_progress(len(paths), desc=f'parallelize: Uploading to table {table_name}...')
    global COUNTER
    COUNTER = Counter(total=len(paths)) # Intitialize a new shared counter. 

    chunks = [paths[i * chunk_size: (i + 1) * chunk_size] for i in range(len(paths) // chunk_size + 1)]
    args = [(chunk, table_name, file_class) for chunk in chunks]
    
    # TODO: Read more about how this works. 
    # https://stackoverflow.com/questions/53751050/multiprocessing-understanding-logic-behind-chunksize 
    # TODO: Read about starmap versus map. Need this for iterable arguments. 
    # TODO: Read about what exactly chunksize is doing.

    n_workers = os.cpu_count() 
    print(f'parallelize: Starting a pool with {n_workers} processes.')
    with Pool(n_workers) as pool:
        results = pool.starmap_async(upload_func, args, chunksize=int(len(chunks) // (2 * n_workers)), error_callback=error_callback)
        results = results.get(None) # Wait for results to be available, with no timeout. 
        pool.close()
        pool.join()
    print() # So the last line of the counter isn't overwritten. 
    print(f'parallelize: {sum(results)} total entries were written to the database.')
    

if __name__ == '__main__':
    
    global DATABASE # Need to declare as global for multiprocessing to work. 
    DATABASE = Database(reflect=False)

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=207, type=int, help='The GTDB version to upload to the SQL database.')
    parser.add_argument('--drop-existing', action='store_true')
    # parser.add_argument('--parallelize', action='store_true')
    args = parser.parse_args()

    global VERSION # Just set the global parameter to reduce argument number. 
    VERSION = args.version 

    global COUNTER
    COUNTER = None
    
    data_dir = os.path.join(DATA_DIR, f'r{VERSION}')

    if args.drop_existing:
        for table_name in DATABASE.table_names[::-1]:
            print(f'Dropping existing table {table_name}.')
            DATABASE.drop(table_name)

    for table_name in DATABASE.table_names:
        print(f'Initializing table {table_name}.')
        DATABASE.create(table_name)

    # DATABASE.drop('annotations_kegg_r207')
    # DATABASE.drop('annotations_pfam_r207')
    # DATABASE.create('annotations_kegg_r207')
    # DATABASE.create('annotations_pfam_r207')

    DATABASE.reflect()

    # NOTE: Table uploads must be done sequentially, i.e. the entire metadata table needs to be up before anything else. 

    print(f'Uploading to the metadata_r{VERSION} table.')
    metadata_paths = glob.glob(os.path.join(data_dir, '*metadata*.tsv')) # This should output the full paths. 
    # upload(metadata_paths, database, f'metadata_r{VERSION}', MetadataFile)
    upload(metadata_paths, f'metadata_r{VERSION}', MetadataFile)

    # Need to upload amino acid and nucleotide data simultaneously.
    print(f'Uploading to the proteins_r{VERSION} table.')
    proteins_aa_dir, proteins_nt_dir = os.path.join(data_dir, 'proteins_aa'), os.path.join(data_dir, 'proteins_nt')
    proteins_aa_paths = [os.path.join(proteins_aa_dir, file_name) for file_name in os.listdir(proteins_aa_dir) if (file_name != 'gtdb_release_tk.log.gz')]
    proteins_nt_paths = [os.path.join(proteins_nt_dir, file_name) for file_name in os.listdir(proteins_nt_dir)]
    paths = [(aa_path, nt_path) for aa_path, nt_path in zip(sorted(proteins_aa_paths), sorted(proteins_nt_paths))]
    # parallelize(paths, upload_proteins, database, f'proteins_r{VERSION}', ProteinsFile)
    parallelize(paths, upload_proteins, f'proteins_r{VERSION}', ProteinsFile)


    print(f'Uploading to the annotations_kegg_r{VERSION} table.')
    annotations_kegg_dir = os.path.join(data_dir, 'annotations_kegg')
    paths = [os.path.join(annotations_kegg_dir, file_name) for file_name in os.listdir(annotations_kegg_dir)]
    # parallelize(path, upload, database, f'annotations_kegg_r{VERSION}', KeggAnnotationsFile)
    # upload(paths, f'annotations_kegg_r{VERSION}', KeggAnnotationsFile)
    parallelize(paths, upload, f'annotations_kegg_r{VERSION}', KeggAnnotationsFile)

    DATABASE.close()
    