import os 
import re
from sqlalchemy import Float, String, Integer
from typing import Dict, List, NoReturn
import pandas as pd 
import numpy as np 

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

    def __init__(self, path:str, gtdb_version:int=None):

        self.path = path 
        self.gtdb_version = gtdb_version
        self.dir_name, self.file_name = os.path.split(path) 
        self.data = None # This will be populated with a DataFrame in child classes. 

    def dataframe(self):
        return self.data

    def entries(self):
        '''Get the file entries in a format which can be easily added to a SQL table.'''
        entries = self.dataframe().to_dict(orient='records')
        for entry in entries:
            # Add the genome ID to the entries as another field if it is present as a File attribute.
            if self.genome_id is not None:
                entry['genome_id'] = self.genome_id
            entry['gtdb_version'] = self.gtdb_version
        return entries


class FastaFile(File):

    def __init__(self, path:str, gtdb_version:int=None):

        super().__init__(path, gtdb_version=gtdb_version) 

        self.n_entries = None
        with open(path, 'r') as f:
            self.content = f.read()

        # Extract the genome ID from the filename. 
        self.genome_id = re.search('GC[AF]_\d{9}\.\d{1}', self.file_name).group(0)


    def parse_header(self, header:str) -> Dict:
        gene_id = header.split('#')[0]
        return {'gene_id':gene_id}

    def headers(self):
        '''Extract all sequence headers stored in a FASTA file.'''
        return list(re.findall(r'^>.*', self.content, re.MULTILINE))

    def sequences(self):
        '''Extract all  sequences stored in a FASTA file.'''
        seqs = re.split(r'^>.*', self.content, flags=re.MULTILINE)[1:]
        # Strip all of the newline characters from the amino acid sequences. 
        seqs = [s.replace('\n', '') for s in seqs]
        return seqs

    def size(self):
        # Avoid re-computing the number of entries each time. 
        if self.n_entries is None:
            self.n_entries = len(self.headers())
        return self.n_entries

    def dataframe(self) -> pd.DataFrame:
        '''Load a FASTA file in as a pandas DataFrame. If the FASTA file is for a particular genome, then 
        add the genome ID as an additional column.'''
        df = [parse_header(header) for header in self.headers()]
        for row, seq in zip(df, self.sequences()):
            row['seq'] = seq
        return pd.DataFrame(df)


class ProteinsFile(FastaFile):

    fields = ['gene_id', 'start', 'stop', 'strand', 'gc_content', 'partial', 'rbs_motif', 'rbs_spacer', 'scaffold_id']

    def __init__(self, path:str, gtdb_version:int=None):

        super().__init__(path, gtdb_version=gtdb_version)

    def parse_header(self, header:str) -> Dict[str, object]:
        '''Parse the header string of an entry in a genome file. Headers are of the form:
        >{gene_id} # {start} # {stop} # {strand} # ID={prodigal_id};partial={partial};start_type={start_type};rbs_motif={rbs_motif};rbs_spacer={rbs_spacer};gc_cont={gc_cont}.
        Full descriptions of each field can be found here: https://github.com/hyattpd/Prodigal/wiki/understanding-the-prodigal-output
        
        :param header: The header string for the sequence.
        :return: A dictionary mapping each field in the header to a value.
        ''' 
        entry = dict()
        pattern = '>([^#]+) # (\d+) # (\d+) # ([-1]+) # (.+)' # Pattern matching the header.
        match = re.match(pattern, header)

        entry['gene_id'] = match.group(1)
        entry['nt_start'] = int(match.group(2))
        entry['nt_stop'] = int(match.group(3))
        entry['strand'] = '-' if match.group(4) == '-1' else '+'
        
        # Iterate over the semicolon-separated information in the final portion of the header. 
        for field, value in [item.split('=') for item in match.group(5).split(';')]:

            if field == 'gc_cont':
                entry['gc_content'] = float(value)
            elif field == 'ID':
                entry['scaffold_id'] = value.split('_')[0]
            elif field in ProdigalFile.fields:
                entry[field] = value

        return entry


class MetadataFile(File):

    fields = {'accession':str, 
        'checkm_completeness':float, 
        'checkm_contamination':float, 
        'coding_bases':int, 
        'coding_density':float, 
        'contig_count':int, 
        'gc_percentage':float, 
        'genome_size':int, 
        'gtdb_taxonomy':str, 
        'l50_contigs':float, 
        'l50_scaffolds':float, 
        'longest_contig':int, 
        'longest_scaffold':int, 
        'ncbi_genome_representation':str, 
        'mean_contig_length':float, 
        'mean_scaffold_length':float,
        'n50_contigs':float, 
        'n50_scaffolds':float, 
        'ncbi_contig_count':int, 
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


    def __init__(self, path:str, gtdb_version:int=None):
        
        super().__init__(path, gtdb_version=gtdb_version)

        data = pd.read_csv(path, delimiter='\t', usecols=list(MetadataFile.fields.keys()), converters={f:get_converter(t) for f, t in MetadataFile.fields.items()})
        data = data.rename(columns={'gc_percentage':'gc_content', 'accession':'genome_id'}) # Fix some of the column names for consistency. 

        taxonomy_data = []
        for taxonomy, genome_id in zip(data['gtdb_taxonomy'], data['genome_id']):
            row = MetadataFile.parse_taxonomy(taxonomy)
            row['genome_id'] = genome_id
        taxonomy_data = pd.DataFrame(taxonomy_data)

        print(data.columns)
        print(taxonomy_data.columns)

        # Merge the parsed taxonomy data and the rest of the metadata, dropping the taxonomy string column. 
        self.data = data.drop(columns='gtdb_taxonomy').merge(taxonomy_data, left_on='genome_id', right_on='genome_id')


class KeggAnnotationsFile(File):

    fields = ['gene_id', 'ko', 'threshold', 'score', 'e_value'] # Define the new column headers. 

    def __init__(self, path:str, gtdb_version:int=None):

        super().__init__(path, gtdb_version=gtdb_version)
        
        # Replace the existing headers in the CSV file with new headers. 
        self.data = pd.read_csv(path, header=0, names=KeggAnnotationsFile.fields) # Read in the CSV file. 

        # Extract the genome ID from the filename. 
        self.genome_id = re.search('GC[AF]_\d{9}\.\d{1}', self.file_name).group(0)



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

    def __init__(self, path:str, gtdb_version:int=None):

        super().__init__(path, gtdb_version=gtdb_version)
        
        # The Pfam annotation files do not contain headers, so need to define them. 
        data = pd.read_csv(path, header=None, names=PfamAnnotationsFile.fields, sep='\t') # Read in the TSV file. 
        # Make sure the data columns match those needed for the table. 
        data = data.rename(columns={'signature_accession':'pfam'})
        self.data = data[['gene_id', 'pfam', 'e_value', 'interpro_accession', 'interpro_description', 'start', 'stop', 'length']]

        # Extract the genome ID from the filename. 
        self.genome_id = re.search('GC[AF]_\d{9}\.\d{1}', self.file_name).group(0)



    

