'''Script for initializing the Find-A-Bug database using the data already present on the microbes.gps.caltech.edu server.'''
import os
os.sys.path.append('../utils/')
import argparse
from utils.database import Database
from utils.files import * 
from tqdm import tqdm

DATA_DIR = '/var/lib/pgsql/data/gtdb/'

def upload_files(database:Database, version:int=None, data_dir:str=None, table_name:str=None, chunk_size:int=100, file_class:File=None):

    file_names = os.listdir(data_dir)

    if chunk_size is None:
        chunks = [file_names]
    else:
        chunks = [file_names[i * chunk_size: (i + 1) * chunk_size] for i in range((len(file_names) // chunk_size) + 1)]
    
    pbar = tqdm(total=len(file_names), desc=f'upload_files: Uploading files to {table_name}.')
    for chunk in chunks:
        entries = []
        for file_name in chunk:
            file = file_class(os.path.join(data_dir, file_name), version=version)
            entries += file.entries()
            pbar.update(1)

        database.bulk_upload(table_name, entries)


def upload_proteins_files(database:Database, version:int=None, aa_data_dir:str=None, nt_data_dir:str=None, chunk_size:int=100):
    '''A function for handling upload of protein sequence files to the database, which is necessary because separate nucleotide and 
    amino acid files need to be combined in a single upload to the proteins table.'''
    # Sort the file name lists, so that the ordering of genome IDs is the same. 
    # NOTE: Can we assume that the ordering of protein sequences is the same within each file? I suspect yes. 
    aa_file_names = sorted(os.listdir(aa_data_dir))
    nt_file_names = sorted(os.listdir(nt_data_dir))
    # print(f'Total amino acid files:', len(aa_file_names))
    # print(f'Total nucleotide files:', len(nt_file_names))
    assert len(aa_file_names) == len(nt_file_names), 'upload_proteins_files: The number of nucleotide and amino acid files should match.' 

    file_names = list(zip(aa_file_names, nt_file_names)) # Combine the different file names into a single list. 

    if chunk_size is None:
        chunks = [file_names]
    else:
        chunks = [file_names[i * chunk_size: (i + 1) * chunk_size] for i in range((len(file_names) // chunk_size) + 1)]
        assert sum([len(chunk) for chunk in chunks]) == len(file_names), 'upload_proteins_files: Chunking files dropped some file names.' 
    
    pbar = tqdm(total=len(file_names), desc=f'upload_files: Uploading files to  proteins.')
    for chunk in chunks:
        entries = []
        for aa_file_name, nt_file_name in chunk:
            aa_file = ProteinsFile(os.path.join(aa_data_dir, aa_file_name), version=version)
            nt_file = ProteinsFile(os.path.join(nt_data_dir, nt_file_name), version=version)
            # print(f'Loading data for files {aa_file.file_name} and {nt_file.file_name}.')
            assert aa_file.size() == nt_file.size(), 'upload_proteins_files: The number of entries in corresponding nucleotide and amino acid files should match.' 
            for aa_entry, nt_entry in zip(aa_file.entries(), nt_file.entries()):
                assert aa_entry['gene_id'] == nt_entry['gene_id'], 'upload_proteins_files: Gene IDs in corresponding amino acid and nucleotide files should match.'  
                entry = aa_entry.copy() # Merge the nucleotide and amino acid entries. 
                entry.update({f:v for f, v in nt_entry.items() if f != 'nt_seq'}) # Nucleotide sequences don't fit in table.
                entries.append(entry)

            pbar.update(1)
        database.bulk_upload('proteins', entries) 



if __name__ == '__main__':
    database = Database(reflect=False)

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default='r207', type=str, help='The GTDB version to upload to the SQL database. Initial version used was r207')
    parser.add_argument('--table-names', default=database.table_names, nargs='+', help='The names of the tables to initialize.')
    parser.add_argument('--drop-existing', type=bool, default=True)
    parser.add_argument('--chunk-size', type=int, default=100, help='The number of files to upload to the database at a time. ')

    args = parser.parse_args()
    
    version_dir = os.path.join(DATA_DIR, f'r{args.version}')

    if args.drop_existing:
        for table_name in args.table_names[::-1]:
            print(f'Dropping existing table {table_name}.')
            database.drop(table_name)

    for table_name in args.table_names:
        print(f'Initializing table {table_name}.')
        database.create(table_name)

    database.reflect()

    if f'metadata_{args.version}' in args.table_names:
        print('Uploading initial data to the metadata table.')
        data_dir = os.path.join(version_dir, 'metadata')
        upload_files(database, version=args.version, data_dir=data_dir, table_name='metadata', file_class=MetadataFile, chunk_size=None)

    if f'proteins_{args.version}' in args.table_names:
        print('Uploading initial data to the proteins table.')
        # Need to upload amino acid and nucleotide data simultaneously.
        aa_data_dir = os.path.join(version_dir, 'proteins', 'amino_acids')
        nt_data_dir = os.path.join(version_dir, 'proteins', 'nucleotides')
        upload_proteins_files(database, version=args.version, aa_data_dir=aa_data_dir, nt_data_dir=nt_data_dir, chunk_size=args.chunk_size) 

    # if 'annotations_pfam' in args.table_names:
    #     print('Uploading initial data to the annotations_pfam table.')
    #     data_dir = os.path.join(version_dir, 'annotations', 'pfam')
    #     upload_files(database, version=args.version, data_dir=data_dir, table_name='annotations_pfam', file_class=PfamAnnotationsFile, chunk_size=args.chunk_size)
    
    if f'annotations_kegg_{args.version}' in args.table_names:
        print('Uploading initial data to the annotations_kegg table.')
        data_dir = os.path.join(version_dir, 'annotations', 'kegg')
        upload_files(database, version=args.version, data_dir=data_dir, table_name='annotations_kegg', file_class=KeggAnnotationsFile, chunk_size=args.chunk_size)

    database.close()
    