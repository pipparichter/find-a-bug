'''Script for initializing the Find-A-Bug database using the data already present on the microbes.gps.caltech.edu server.'''
import argparse
from utils.database import Database
from utils import DATA_DIR
from utils.files import * 
import os

def upload_files(database:Database, release:int=None, data_dir:str=None, table_name:str=None, chunk_size:int=100, file_class:File=None):

    file_names = file_name in os.listdir(data_dir)
    chunks = [file_names[i * chunk_size: (i + 1) * chunk_size] for i in range((len(file_names) // chunk_size) + 1)]
    for chunk in tqdm(chunks, desc=f'upload_files: Uploading files to {table_name}.'):
        entries = []
        for file_name in chunks:
            file = file_class(os.path.join(data_dir, file_name), release=release)
            entries += file.entries()
        database.bulk_upload(table_name, entries)


if __name__ == '__main__':
    database = Database(reflect=False)

    parser = argparse.ArgumentParser()
    parser.add_argument('--release', default=207, type=int, help='The GTDB release to upload to the SQL database. Initial release used was r207')
    parser.add_argument('--table-names', default=database.table_names, nargs='+', help='The names of the tables to initialize. Must be one of the non-history tables defined in utils/tables.py.')
    parser.add_argument('--drop-existing', type=bool, default=True)
    parser.add_argument('--chunk-size', type=int, default=100, help='The number of files to upload to the database at a time. ')

    args = parser.parse_args()
    
    release_dir = os.path.join(DATA_DIR, f'r{args.release}')

    for table_name in args.table_names:
        print(f'Initializing table {table_name}.')
        database.create(table_name, args.drop_existing)
    
    if 'annotations_pfam' in args.table_names:
        print('Uploading initial data to the annotations_pfam table.')
        data_dir = os.path.join(release_dir, 'annotations', 'pfam')
        upload_files(database, release=args.release, data_dir=data_dir, table_name='annotations_pfam', file_class=PfamAnnotationsFile, chunk_size=args.chunk_size)
    
    if 'annotations_kegg' in args.table_names:
        print('Uploading initial data to the annotations_kegg table.')
        data_dir = os.path.join(release_dir, 'annotations', 'kegg')
        upload_files(database, release=args.release, data_dir=data_dir, table_name='annotations_kegg', file_class=KeggAnnotationsFile, chunk_size=args.chunk_size)

    if 'metadata' in args.table_names:
        print('Uploading initial data to the metadata table.')
        data_dir = os.path.join(release_dir, 'metadata')
        upload_files(database, release=args.release, data_dir=data_dir, table_name='metadata', file_class=MetadataFile, chunk_size=1)
    
    if 'proteins' in args.table_names:
        print('Uploading initial data to the proteins table.')
        data_dir = os.path.join(release_dir, 'proteins', 'bacteria')
        upload_files(database, release=args.release, data_dir=data_dir, table_name='proteins', file_class=ProteinsFile, chunk_size=args.chunk_size)
        data_dir = os.path.join(release_dir, 'proteins', 'archaea')
        upload_files(database, release=args.release, data_dir=data_dir, table_name='proteins', file_class=ProteinsFile, chunk_size=args.chunk_size)

    database.close()
    