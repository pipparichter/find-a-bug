'''Tests for the gtdb_r207_metadata table in the Find-A-Bug SQL database.'''
import unittest



class TestGTDBMetadata(unittest.TestCase):
    '''Class for handling tests of the gtdb_r207_metadata table.'''

    def test_all_genome_ids_in_gtdb_r207_amino_acid_seqs_are_represented(self):
        '''Test to make sure that all genomes present in the gtdb_r207_amino_acids table have
        associated metadata in the metadata table.'''
        pass