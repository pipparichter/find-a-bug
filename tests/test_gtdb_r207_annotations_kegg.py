'''Tests for the gtdb_r207_annotations_kegg table in the Find-A-Bug SQL database.'''
import sys
sys.path.append('/home/prichter/find-a-bug/')

import unittest
from data.utils import *
from sqlalchemy import create_engine

ANNOTATIONS_PATH = load_config_paths()['annotations_path']

# Read in the config file, which is in the project root directory. 
config = configparser.ConfigParser()
# with open('/home/prichter/Documents/find-a-bug/find-a-bug.cfg', 'r', encoding='UTF-8') as f:
with open(os.path.join(os.path.dirname(__file__), '../',  'find-a-bug.cfg'), 'r', encoding='UTF-8') as f:
    config.read_file(f)

URL = '{dialect}+{driver}://{user}:{password}@{host}/{name}'.format(**dict(config.items('db')))
ENGINE = create_engine(URL)

ANNOTATIONS_PATH = load_config_paths()['annotations_path'] # Where the annotations are located. 
TABLE_NAME = 'gtdb_r207_annotations_kegg'


def count_annotations():
    '''Counts the total number of annotations stored in the directory. If this is slow, 
    it's probably worth calculating once and storing this information in a file.'''
    annotation_files = os.listdir(ANNOTATIONS_PATH)
    count = 0
    for file in os.listdir(ANNOTATIONS_PATH): # Only gives the filename. 
        path = os.path.join(ANNOTATIONS_PATH, file)
        count += csv_size(path) 
    return count


class TestGTDBAnnotationsKegg(unittest.TestCase):
    '''Class for handling tests of the gtdb_r207_annotations_kegg table.'''

    # I don't think every sequence will be represented. 
    def test_all_gene_ids_in_gtdb_r207_amino_acid_seqs_are_represented(self):
        '''Test to make sure that all genes present in the gtdb_r207_amino_acids table have associated annotations.'''
        pass

    def test_annotation_ids_are_correct(self):
        '''Make sure that all IDs in the range (0, [number of annotations]) are present in the database.'''
        # First get the total number of annotations from the data path. 
        num_annotations = count_annotations()
        table_size = get_table_size(ENGINE, TABLE_NAME)
        assert num_annotations == table_size, f'Expected {num_annotations} entries in {TABLE_NAME}, but got {table_size}.'  


if __name__ == '__main__':
    unittest.main()
