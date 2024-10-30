import pandas as pd 
import numpy as np 
import os 
import glob
import shutil
from tqdm import tqdm
from utils.files import * 


DATA_DIR = '/var/lib/pgsql/data/gtdb/'


def count_total_proteins(data_dir:str=os.path.join(DATA_DIR, 'proteins_aa')):
    count = 0 
    for file_name in tqdm(os.listdir(data_dir), desc='count_total_proteins'):
        path = os.path.join(data_dir, file_name)
        file = ProteinsFile(path)
        count += file.size()
    return count

total = count_total_proteins()
print('Number of proteins in the table:', total)
