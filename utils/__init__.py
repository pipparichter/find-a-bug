import os 

MAX_SEQ_LENGTH = 20000 # The maximum number of amino acids allowed for a protein sequence. 
GENOME_ID_LENGTH = 20 # Length of the GTDB genome accessions. 
GENE_ID_LENGTH = 50 # Approximate length of GTDB gene accessions. 
DEFAULT_STRING_LENGTH = 50

# Define some key directories. TODO: Might want to dump this in a designated configuration file. 
DATA_DIR = '/var/lib/pgsql/data/gtdb/'


def get_current_release() -> int:
    '''Get the most recent GTDB release in the database.'''
    path = os.dirname(os.path.abspath(__file__))
    release = pd.read_csv(os.path.join(path, 'updates.csv'), index_col=0).release.values[-1]
    return release
