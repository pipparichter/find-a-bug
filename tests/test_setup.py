import pandas as pd 
import numpy as np 
import os 
import glob
import shutil
from tqdm import tqdm
from utils.files import * 

version = 'r207'
DATA_DIR = f'/var/lib/pgsql/data/gtdb/{version}'


def is_fasta(file_name:str):
    is_nt = ('.fna' in file_name)
    is_aa = ('.faa' in file_name)
    return is_nt or is_aa

def count_total_proteins(data_dir:str=os.path.join(DATA_DIR, 'proteins_aa')):
    count = 0 
    for file_name in tqdm(os.listdir(data_dir), desc='count_total_proteins'):
        if is_fasta(file_name):
            path = os.path.join(data_dir, file_name)
            file = ProteinsFile(path)
            count += file.size()
    return count

total = count_total_proteins()
print('Number of proteins in the table:', total)
