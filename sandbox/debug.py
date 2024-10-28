import pandas as pd 
import numpy as np 
import os 

# print('Current working directory:', os.getcwd())

failing_genome_ids = np.loadtxt('failing_genome_ids.txt', dtype=str)

DATA_DIR = '/var/lib/pgsql/data/gtdb/'
LOG_DIR = os.path.join(os.getcwd(), '../scripts/log/')

genome_ids_in_directory = os.listdir(os.path.join(DATA_DIR, 'r207', 'proteins_aa'))
# Remove the file extensions and prefixes...
genome_ids_in_directory = [file_name.replace('.faa.gz', '') for file_name in genome_ids_in_directory]
genome_ids_in_directory = [genome_id.replace('RS_', '').replace('GB_', '') for genome_id in genome_ids_in_directory]

genome_ids_missing = []
for genome_id in failing_genome_ids:
    if not (genome_id in genome_ids_in_directory):
        print(genome_id, 'missing in proteins_aa directory.')
        genome_ids_missing.append(genome_id)

print(len(genome_ids_missing), 'total genome IDs are not in the proteins_aa directory.')