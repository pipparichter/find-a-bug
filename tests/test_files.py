import unittest
import os 
import pandas as pd 
import numpy as np 
import glob
from utils.files import * 
import subprocess
from parameterized import parameterized 

DATA_DIR = os.path.join(os.getcwd(), 'data')
GENOME_IDS = pd.read_csv(os.path.join(DATA_DIR, 'genome_ids.csv'), comment='#', index_col=0).values.ravel().tolist()[:1]


# NOTE: zip fails silently if list sizes are unequal. I should make sure to check this in the setup.py script. 

def get_file_path(data_dir:str, genome_id:str):
    '''Get the path to the file corresponding to the input genome ID in the specified directory.'''
    file_path = glob.glob(os.path.join(data_dir, f'*{genome_id}*'))[0]
    return file_path


def load_files(data_dir:str, file_class) -> List[File]:
    '''Load all of the File objects for the specified directory to avoid some overhead.'''
    files = []
    for genome_id in GENOME_IDS:
        file_path = get_file_path(data_dir, genome_id)
        files.append(file_class(file_path))
    return files


class TestProteinsFile(unittest.TestCase):
    '''Class for testing the File objects defines in utils/files.py.'''
    aa_data_dir = os.path.join(DATA_DIR, 'proteins_aa')
    nt_data_dir = os.path.join(DATA_DIR, 'proteins_nt')

    aa_files = load_files(aa_data_dir, ProteinsFile)
    nt_files = load_files(nt_data_dir, ProteinsFile)

    @staticmethod
    def count_entries(path:str):
        '''Count the number of entries in a FASTA file by counting the number of '>' characters in the file.'''
        content = read(path)
        return content.count('>')

    @parameterized.expand(aa_files + nt_files)
    def test_all_sequences_loaded(self, file:ProteinsFile):
        self.assertEqual(file.size(), TestProteinsFile.count_entries(file.path))

    @parameterized.expand(aa_files + nt_files)
    def test_dataframe_size_is_correct(self, file:ProteinsFile):
        df = file.dataframe()
        self.assertEqual(len(df), TestProteinsFile.count_entries(file.path))

    @parameterized.expand(aa_files + nt_files)
    def test_correct_number_of_entries(self, file:ProteinsFile):
        entries = file.entries()
        self.assertEqual(len(entries), TestProteinsFile.count_entries(file.path))


class TestKeggAnnotationsFile(unittest.TestCase):
    '''Class for testing the File objects defines in utils/files.py.'''
    data_dir = os.path.join(DATA_DIR, 'annotations_kegg')
    files = load_files(data_dir, KeggAnnotationsFile)

    @staticmethod
    def count_entries(path:str):
        '''Count the number of lines in a metadata file.'''
        n = len(read(path).strip().split('\n'))
        return n - 2 # Subtract 1 for the header line and 1 for the dashed line. 
    
    @parameterized.expand(files)
    def test_all_sequences_loaded(self, file:KeggAnnotationsFile):
        self.assertEqual(file.size(), TestKeggAnnotationsFile.count_entries(file.path))

    @parameterized.expand(files)
    def test_dataframe_size_is_correct(self, file:KeggAnnotationsFile):
        df = file.dataframe()
        self.assertEqual(len(df), TestKeggAnnotationsFile.count_entries(file.path))

    @parameterized.expand(files)
    def test_correct_number_of_entries(self, file:KeggAnnotationsFile):
        entries = file.entries()
        self.assertEqual(len(entries), TestKeggAnnotationsFile.count_entries(file.path))

    @parameterized.expand(files)
    def test_columns_are_correct(self, file:KeggAnnotationsFile):
        correct_n_columns = len(KeggAnnotationsFile.fields)
        n_columns = len(file.data.columns)
        self.assertEqual(n_columns, correct_n_columns, f"Expected {correct_n_columns}, but found {n_columns}. Columns in the data attribute are: {', '.join(file.data.columns)}")
        self.assertTrue(np.all(np.isin(file.data.columns, KeggAnnotationsFile.fields)))
    
    @parameterized.expand(files)
    def test_no_nan_values(self, file:KeggAnnotationsFile):
        self.assertTrue(not np.any(file.data.isnull()), f'Found {file.data.isnull().values.sum()} null values in the KeggAnnotationsFile.')




class TestMetadataFile(unittest.TestCase):

    file = MetadataFile(os.path.join(DATA_DIR, 'archaea_metadata.tsv'), reps_only=False)

    @staticmethod
    def count_entries(path:str):
        '''Count the number of lines in a metadata file.'''
        with open(path, 'r') as f:
            n = len(f.readlines())
        return n - 1 # Subtract 1 for the header line. 
    
    def test_dataframe_size_is_correct(self, file:MetadataFile=file):
        df = file.dataframe()
        self.assertEqual(len(df), TestMetadataFile.count_entries(file.path))

    def test_correct_number_of_entries(self, file:MetadataFile=file):
        entries = file.entries()
        self.assertEqual(len(entries), TestMetadataFile.count_entries(file.path))


if __name__ == '__main__':
    unittest.main()