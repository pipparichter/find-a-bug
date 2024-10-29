import pandas as pd 
import numpy as np 
import os 
import glob

# print('Current working directory:', os.getcwd())

failing_genome_ids = np.loadtxt('failing_genome_ids.txt', dtype=str)

DATA_DIR = '/var/lib/pgsql/data/gtdb/'
LOG_DIR = os.path.join(os.getcwd(), '../scripts/log/')

# genome_ids_in_directory = os.listdir(os.path.join(DATA_DIR, 'r207', 'proteins_aa'))
# # Remove the file extensions and prefixes...
# genome_ids_in_directory = [file_name.replace('_protein.faa.gz', '') for file_name in genome_ids_in_directory]
# genome_ids_in_directory = [genome_id.replace('RS_', '').replace('GB_', '') for genome_id in genome_ids_in_directory]

# genome_ids_missing = []
# for genome_id in failing_genome_ids:
#     if not (genome_id in genome_ids_in_directory):
#         print(genome_id, 'missing in proteins_aa directory.')
#         genome_ids_missing.append(genome_id)

# print(len(genome_ids_missing), 'total genome IDs are not in the proteins_aa directory.')

# These are all the genome IDs which have missing genes in the database. 
genome_ids_with_missing_genes = ['GCF_015244675.1', 'GCA_013140765.1', 'GCA_010032545.1', 'GCA_011364235.1', 'GCA_018401055.1', 'GCF_015694425.1', 'GCF_015245355.1', 'GCA_001779505.1', 'GCA_018608425.1', 'GCA_011364155.1', 'GCA_003157385.1', 'GCF_002909445.1', 'GCA_011364525.1', 'GCA_905182715.1', 'GCA_011364265.1', 'GCA_900545805.1', 'GCF_000709085.1', 'GCF_015244745.1', 'GCA_012518835.1', 'GCA_017467305.1', 'GCF_015265435.1', 'GCF_003946115.1', 'GCA_011364225.1', 'GCA_002313895.1', 'GCF_001675285.1', 'GCA_011364305.1', 'GCF_902499045.1', 'GCA_011364105.1']
for genome_id in genome_ids_with_missing_genes:
    if len(glob.glob(os.path.join(DATA_DIR, 'r207', 'proteins_aa'), f'*{genome_id}_protein.faa.gz')) < 1:
        print(genome_id, 'is missing in the source directory.')
    else:
        print(genome_id, 'is present in the source directory.')