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


def extract(archive:tarfile.TarFile, name:str, output_dir:str='.') -> str:
    '''Read the file specified by member from the compressed archive specified at path, and write the
    output to a file at path. 
    
    :param archive: An open tar archive. 
    :param name: The name of the archive member. Note that the name should be a relative path to the 
        member, i.e. archaea/RS_GCA_*.faa.gz.
    :param output_dir: The path where the extracted file will be written. 
    :returns: The path to the output file. 
    '''

    member = archive.getmember(name)
    if member.isfile():
        content = archive.extractfile(member).read()

        output_file_path = os.path.join(output_dir, os.path.basename(name))
        with open(output_file_path, 'wb') as f:
            f.write(content)

        return output_file_path
    else:
        return None



def upload_files(archive:tarfile.TarFile, database:Database, 
                    version:int=None, 
                    table_name:str=None, 
                    chunk_size:int=100, 
                    file_class:File=None, 
                    data_dir:str=None):
    '''Upload files stored in a zip archive to the Find-A-Bud database.

    :param archive: The tar archive where the files are stored. 
    :param database: The Database object which connects to the Find-A-Bug database. 
    :param table_name: The name of the table in the database where the data will be uploaded. 
    :param chunk_size: The size of the batches for uploading the data. 
    :param version: The version of GTDB the data belongs to. 
    :param data_dir: The directory where all the data is located. Thie is pretty much just used for 
        writing temporary files. 
    :param file_class: The type of file being uploaded to the database. 
    '''

    names = archive.getnames()
    chunks = [names] if (chunk_size is None) else [names[i * chunk_size: (i + 1) * chunk_size] for i in range((len(names) // chunk_size) + 1)]
    
    pbar = tqdm(total=len(names), desc=f'upload_files: Uploading files to {table_name}.')
    for chunk in chunks:
        entries = []
        for name in chunk:
            path = extract(archive, name, output_dir=data_dir)
            file = file_class(path, version=version)
            entries += file.entries()

            os.remove(path) # Remove the file which was temporarily extracted from the archive. 
            pbar.update(1)

        database.bulk_upload(table_name, entries)



def upload_proteins_files(aa_archive:tarfile.TarFile, nt_archive:tarfile.TarFile, database:Database, 
                            version:int=None, 
                            chunk_size:int=100, 
                            data_dir:str=None):
    '''A function for handling upload of protein sequence files to the database, which is necessary because separate 
    nucleotide and amino acid files need to be combined in a single upload to the proteins table.
    
    :param aa_archive: The tar archive where the amino acid FASTA files are stored. 
    :param nt_archive: The tar archive where the nucleotide FASTA files are stored. 
    :param database: The Database object which connects to the Find-A-Bug database. 
    :param chunk_size: The size of the batches for uploading the data. 
    :param version: The version of GTDB the data belongs to. 
    :param data_dir: The directory where all the data is located. Thie is pretty much just used for 
        writing temporary files. 
    '''
    # Sort the file name lists, so that the ordering of genome IDs is the same. 
    # NOTE: Can we assume that the ordering of protein sequences is the same within each file? I suspect yes. 
    aa_names, nt_names = sorted(aa_archive.getnames()), sorted(nt_archive.getnames())
    aa_names = [name for name in aa_names if '.log' not in name] # There's a random extra file in with the amino acids.
    assert len(aa_names) == len(nt_names), f'upload_proteins_files: The number of nucleotide and amino acid files should match. Found {len(aa_names)} amino acid files and {len(nt_names)} nucleotide files.' 

    names = list(zip(aa_names, nt_names)) # Combine the different file names into a single list. 
    chunks = [names] if (chunk_size is None) else [names[i * chunk_size: (i + 1) * chunk_size] for i in range((len(names) // chunk_size) + 1)]
    
    pbar = tqdm(total=len(names), desc=f'upload_files: Uploading files to proteins_r{version}.')

    # I think multiprocess is having a hard time dealing with the open archives. 

    def main(chunk:List[Tuple[str, str]]):
        entries = []
        for aa_name, nt_name in chunk:
            nt_path, aa_path = extract(nt_archive, nt_name, output_dir=data_dir), extract(aa_archive, aa_name, output_dir=data_dir)
            if (aa_path is None) and (nt_path is None):
                continue 

            nt_file, aa_file = ProteinsFile(nt_path, version=version), ProteinsFile(aa_path, version=version)
            assert aa_file.size() == nt_file.size(), 'upload_proteins_files: The number of entries in corresponding nucleotide and amino acid files should match.' 
            
            for aa_entry, nt_entry in zip(aa_file.entries(), nt_file.entries()):
                assert aa_entry['gene_id'] == nt_entry['gene_id'], 'upload_proteins_files: Gene IDs in corresponding amino acid and nucleotide files should match.'  
                entry = aa_entry.copy() # Merge the nucleotide and amino acid entries. 
                entry.update({f:v for f, v in nt_entry.items() if f != 'nt_seq'}) # Nucleotide sequences don't fit in table.
                entries.append(entry)

            # Clean up the temporary files. 
            os.remove(nt_path)
            os.remove(aa_path)
            pbar.update(1)

        database.bulk_upload(f'proteins_r{args.version}', entries) 
    
    # TODO: Read more about how this works. 
    # https://stackoverflow.com/questions/53751050/multiprocessing-understanding-logic-behind-chunksize 
    pool = Pool(os.cpu_count())
    pool.map(main, chunks, chunksize=len(chunks) // os.cpu_count())
    pool.close()



if __name__ == '__main__':
    database = Database(reflect=False)

    parser = argparse.ArgumentParser()
    parser.add_argument('--version', default=207, type=int, help='The GTDB version to upload to the SQL database. Initial version used was r207')
    parser.add_argument('--drop-existing', action='store_true')

    args = parser.parse_args()
    
    data_dir = os.path.join(DATA_DIR, f'r{args.version}')

    if args.drop_existing:
        for table_name in database.table_names[::-1]:
            print(f'Dropping existing table {table_name}.')
            database.drop(table_name)

    for table_name in database.table_names:
        print(f'Initializing table {table_name}.')
        database.create(table_name)

    database.reflect()

    print(f'Uploading data to the metadata_r{args.version} table.')
    metadata_archive_paths = glob.glob(os.path.join(data_dir, '*metadata*')) # This should output the full paths. 
    for path in metadata_archive_paths:
        with tarfile.open(path, 'r:gz') as archive:
            upload_files(archive, database, version=args.version, data_dir=data_dir, table_name=f'metadata_r{args.version}', file_class=MetadataFile, chunk_size=None)

    print(f'Uploading data to the proteins_r{args.version} table.')
    # Need to upload amino acid and nucleotide data simultaneously.
    aa_archive_path, nt_archive_path = os.path.join(data_dir, 'proteins_aa.tar.gz'), os.path.join(data_dir, 'proteins_nt.tar.gz')
    with tarfile.open(aa_archive_path, 'r:gz') as aa_archive, tarfile.open(nt_archive_path, 'r:gz') as nt_archive:   
    
    
    upload_proteins_files(aa_archive, nt_archive, database, version=args.version, data_dir=data_dir) 

    #     print('Uploading initial data to the annotations_pfam table.')
    #     data_dir = os.path.join(data_dir, 'annotations', 'pfam')
    #     upload_files(database, version=args.version, data_dir=data_dir, table_name='annotations_pfam', file_class=PfamAnnotationsFile, chunk_size=args.chunk_size)

    print(f'Uploading data to the annotations_kegg_r{args.version} table.')
    kegg_annotation_archive_path = os.path.join(data_dir, 'annotations_kegg.tar.gz')
    with tarfile.open(kegg_annotation_archive_path, 'r:gz') as archive:
        upload_files(archive, database, version=args.version, data_dir=data_dir, table_name=f'annotations_kegg_r{args.version}', file_class=KeggAnnotationsFile, chunk_size=100)

    database.close()
    