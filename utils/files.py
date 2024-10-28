import os 
import re
import io
from sqlalchemy import Float, String, Integer
from typing import Dict, List, NoReturn
import pandas as pd 
import numpy as np
from utils.tables import MAX_SEQ_LENGTH
import gzip 

# NOTE: Donnie mentioned that pre-compiling regex expressions might speed things up quite a bit. 

def compressed(path:str):
    file_name = os.path.basename(path)
    ext = file_name.split('.')[-1]
    return ext == 'gz'


def read(path:str) -> str:
    '''Reads in a compressed or uncompressed file (detected automatically) as a string of text.'''
    try:
        if compressed(path):
            f = gzip.open(path, 'rt')
        else:
            f = open(path, 'r')
        content = f.read()
        f.close()
        return content
    except Exception as err:
        print(f'read: Problem reading file {path}: {err}')

def get_converter(dtype):
    '''Function for getting type converters to make things easier when reading in the metadata files.'''
    if dtype == str:
        def converter(val):
            return str(val)
    elif dtype == int:
        def converter(val):
            if val == 'none':
                return -1
            else:
                return int(val)
    elif dtype == float:
        def converter(val):
            if val == 'none':
                return -1.0
            else:
                return float(val)
    return converter


class File():
    
    genome_id_pattern = re.compile(r'GC[AF]_\d{9}\.\d{1}')

    def __init__(self, path:str, version:int=None):

        self.data = None # This will be populated in most child classes.
        self.path = path 
        self.version = version
        self.dir_name, self.file_name = os.path.split(path) 
        try:
            # Extract the genome ID from the filename. This should take care of removing the prefix. 
            self.genome_id = re.search('GC[AF]_\d{9}\.\d{1}', self.file_name).group(0) 
        except: # This will break for the MetadataFile objects. 
            self.genome_id = None

    def dataframe(self) -> pd.DataFrame:
        '''Represent the underlying data as a DataFrame.'''
        return self.data

    # TODO: Should make a concerted effort to make sure that this is not a performance bottleneck. 
    def entries(self):
        '''Get the file entries in a format which can be easily added to a SQL table.'''
        entries = self.dataframe().to_dict(orient='records')
        for entry in entries:
            # Add the genome ID to the entries as another field if it is present as a File attribute.
            if self.genome_id is not None:
                entry['genome_id'] = self.genome_id
            entry['version'] = self.version
        return entries


class ProteinsFile(File):

    # Pre-compiling the regex patterms might marginally speed things up. 
    header_pattern = re.compile(r'>([^#]+) # (\d+) # (\d+) # ([-1]+) # (.+)')  # Pattern matching the header.
    new_entry_pattern = re.compile(r'^>.*', re.MULTILINE)
    fields = ['gene_id', 'start', 'stop', 'strand', 'gc_content', 'partial', 'rbs_motif', 'scaffold_id'] # Just the fields in the headers. 


    def __init__(self, path:str, version:int=None):

        super().__init__(path, version=version) 

        # Detect the file type, indicating it contains nucleotides or amino acids. 
        if 'fna' in self.file_name:
            self.type_ = 'nt'
        elif 'faa' in self.file_name:
            self.type_ = 'aa'

        # I think it is not a good idea to store all the content as an attribute for the sake of limiting memory consumption. 
        content = read(path) # Handles compressed and non-compressed files. Expecting this to the bottleneck. 

        self.headers = ProteinsFile.get_headers(content) # This stores the raw headers (not parsed).
        if (self.type_ == 'nt'):
            seqs = ProteinsFile.get_seqs(content)
            self.stop_codons = ProteinsFile.get_stop_codons(seqs)
            self.start_codons = ProteinsFile.get_start_codons(seqs)
            self.seqs = None
        elif (self.type_ == 'aa'):
            self.seqs = ProteinsFile.get_seqs(content)
            self.start_codons = None
            self.stop_codons = None

    @staticmethod
    def parse_header(header:str) -> Dict[str, object]:
        '''Parse the header string of an entry in a genome file. Headers are of the form:
        >{gene_id} # {start} # {stop} # {strand} # ID={prodigal_id};partial={partial};start_type={start_type};rbs_motif={rbs_motif};rbs_spacer={rbs_spacer};gc_cont={gc_cont}.
        Full descriptions of each field can be found here: https://github.com/hyattpd/Prodigal/wiki/understanding-the-prodigal-output
        
        :param header: The header string for the sequence.
        :return: A dictionary mapping each field in the header to a value.
        ''' 
        entry = dict()
        match = re.match(ProteinsFile.header_pattern, header)

        entry['gene_id'] = match.group(1)
        entry['start'] = int(match.group(2))
        entry['stop'] = int(match.group(3))
        entry['strand'] = '-' if match.group(4) == '-1' else '+'
        
        # Iterate over the semicolon-separated information in the final portion of the header. 
        for field, value in [item.split('=') for item in match.group(5).split(';')]:

            if field == 'gc_cont':
                entry['gc_content'] = float(value)
            elif field == 'ID':
                entry['scaffold_id'] = int(value.split('_')[0])
            elif field in ProteinsFile.fields:
                entry[field] = value
        return entry

    @staticmethod
    def get_headers(content:str) -> List[str]:
        '''Extract all sequence headers stored in a FASTA file.'''
        return list(re.findall(ProteinsFile.new_entry_pattern, content))

    @staticmethod
    def get_seqs(content:str) -> List[str]:
        '''Extract all  sequences stored in a FASTA file.'''
        seqs = re.split(ProteinsFile.new_entry_pattern, content)[1:]
        # Strip all of the newline characters from the amino acid sequences. 
        seqs = [s.replace('\n', '') for s in seqs]
        return seqs

    # TODO: I need to make sure I don't need to take the reverse compliments if the nucleotide sequence is on the reverse strand. 
    @staticmethod
    def get_start_codons(seqs:List[str]) -> List[str]:
        return [seq[:3] for seq in seqs]
    
    @staticmethod
    def get_stop_codons(seqs:List[str]) -> List[str]:
        return [seq[-3:] for seq in seqs]

    def size(self):
        # Avoid re-computing the number of entries each time. 
        return len(self.headers)

    def dataframe(self) -> pd.DataFrame:
        '''Load the data conteined in the file as a pandas DataFrame.'''
        df = pd.DataFrame([self.parse_header(header) for header in self.headers])

        if (self.type_ == 'aa'):
            df['seq'] = self.seqs
        if (self.type_ == 'nt'):
            df['stop_codon'] = self.stop_codons 
            df['start_codon'] = self.start_codons

        return pd.DataFrame(df)



class MetadataFile(File):

    fields = {'accession':str, 
        'checkm_completeness':float, 
        'checkm_contamination':float, 
        'coding_bases':int, 
        'coding_density':float, 
        'contig_count':int, 
        'gc_percentage':float, 
        'genome_size':int,
        'protein_count':int, 
        'gtdb_taxonomy':str, 
        'l50_contigs':float, 
        'l50_scaffolds':float, 
        'longest_contig':int, 
        'gtdb_representative':str, # This is either 't' or 'f'
        'longest_scaffold':int, 
        'ncbi_genome_representation':str, 
        'mean_contig_length':float, 
        'mean_scaffold_length':float,
        'n50_contigs':float, 
        'n50_scaffolds':float, 
        'ncbi_contig_count':int, 
        'trna_selenocysteine_count':int,
        'ncbi_contig_n50':float}

    @staticmethod
    def parse_taxonomy(taxonomy:str) -> pd.DataFrame:
        '''Takes a taxonomy string as input, and parses it into a dictionary mapping the new column names to the values.'''
        map_ = {'o':'gtdb_order', 'd':'gtdb_domain', 'p':'gtdb_phylum', 'c':'gtdb_class', 'f':'', 'g':'gtdb_genus', 's':'gtdb_species'}
        parsed = {t:'none' for t in map_.values()}
        if taxonomy == 'none': # This is an edge case. Just fill in all none values if this happens. 
            rows.append(new_row)
        else:
            for t in taxonomy.strip().split(';'):
                flag, entry = t[0], t[3:] # Can't use split('__') because of edge cases where there is a __ in the species name. 
                if flag in map_.keys() and len(entry) > 0: # Also handles case of empty entry. 
                    parsed[map_[flag]] = entry
        return parsed


    def __init__(self, path:str, version:int=None, reps_only:bool=True):
        
        super().__init__(path, version=version)

        content = io.StringIO(read(path)) # Read the file into a IO stream.
        data = pd.read_csv(content, delimiter='\t', usecols=list(MetadataFile.fields.keys()), converters={f:get_converter(t) for f, t in MetadataFile.fields.items()})
        
        if reps_only: # Remove all genomes which are not GTDB representatives. 
            data = data[data.gtdb_representative.str.match('t')]
        data = data.drop(columns='gtdb_representative') # Don't need this column after filtering. 
        data = data.rename(columns={'gc_percentage':'gc_content', 'accession':'genome_id', 'trna_selenocysteine_count':'sec_trna_count'}) # Fix some of the column names for consistency. 

        taxonomy_data = []
        for taxonomy, genome_id in zip(data['gtdb_taxonomy'], data['genome_id']):
            row = MetadataFile.parse_taxonomy(taxonomy)
            row['genome_id'] = genome_id
            taxonomy_data.append(row)
        taxonomy_data = pd.DataFrame(taxonomy_data)

        # Merge the parsed taxonomy data and the rest of the metadata, dropping the taxonomy string column. 
        data = data.drop(columns='gtdb_taxonomy').merge(taxonomy_data, left_on='genome_id', right_on='genome_id')
        data['genome_id'] = data.genome_id.str.replace(r'GB_', '').str.replace(r'RS_', '') # Remove the prefixes from the genome IDs. 
        self.data = data

        # self.data = data[~pd.isnull(data.genome_id)] # I think some of these are None, which is messing things up. 


class KeggAnnotationsFile(File):

    fields = ['gene_id', 'ko', 'threshold', 'score', 'e_value'] # Define the new column headers. 

    def __init__(self, path:str, version:int=None):

        super().__init__(path, version=version)
        
        # Replace the existing headers in the CSV file with new headers. 
        content = io.StringIO(read(path)) # Read the file into a IO stream.
        data = pd.read_csv(content, header=0, names=['#'] + KeggAnnotationsFile.fields, sep='\t', low_memory=False) # Read in the CSV file. 
        self.data = data.drop(columns=['#']) # "#" column marks where E-value exceeds the threshold. 

class PfamAnnotationsFile(File):

    fields = ['gene_id', 
        'digest', 
        'length', 
        'analysis', 
        'signature_accession', 
        'signature_description', 
        'start', 
        'stop', 
        'e_value', 
        'match_status', 
        'data', 
        'interpro_accession', 
        'interpro_description']

    def __init__(self, path:str, version:int=None):

        super().__init__(path, version=version)
        
        # The Pfam annotation files do not contain headers, so need to define them. 
        content = io.StringIO(read(path)) # Read the file into a IO stream.
        data = pd.read_csv(content, header=None, names=PfamAnnotationsFile.fields, sep='\t') # Read in the TSV file. 
        # Make sure the data columns match those needed for the table. 
        data = data.rename(columns={'signature_accession':'pfam'})
        self.data = data[['gene_id', 'pfam', 'e_value', 'interpro_accession', 'interpro_description', 'start', 'stop', 'length']]




    

