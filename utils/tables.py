import os
from sqlalchemy import inspect
import sqlalchemy
import pandas as pd 
import numpy as np
import sqlalchemy.orm
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy import String, Integer, ForeignKey, PrimaryKeyConstraint, Float, ForeignKeyConstraint, Text, CHAR
from sqlalchemy.orm import DeclarativeBase, relationship, mapped_column
from sqlalchemy.ext.declarative import DeferredReflection
from typing import List, Dict, Set

MAX_SEQ_LENGTH = 50000 # The maximum number of amino acids allowed for a protein sequence. 
GENOME_ID_LENGTH = 20 # Length of the GTDB genome accessions. 
GENE_ID_LENGTH = 50 # Approximate length of GTDB gene accessions. 
DEFAULT_STRING_LENGTH = 50

# TODO: Read https://www.geeksforgeeks.org/sqlalchemy-orm-declaring-mapping/

# NOTE: Useful documentation for managing relationships here: https://docs.sqlalchemy.org/en/20/orm/relationship_api.html

# NOTE: The back_populates parameter indicates the name of a relationship() on the related class that will be synchronized 
# with this one. It is usually expected that the relationship() on the related class also refer to this one. This allows objects 
# on both sides of each relationship() to synchronize state changes.

# NOTE: Because the relationship from amino acid sequences to annotations should be one-to-one, should specify uselist=False.
# NOTE: The viewonly=True specifies that the relationship should not be used for persistence operations, only for accessing values.

# NOTE: I think I am going to make the design choice to have the Proteins table as the primary table, and access everything else from there. 
# There should always be a many-to-one (proteins to Metadata) or one-to-many (proteins to annotations) relationship.


class Base(DeclarativeBase):
    pass 

class Reflected(DeferredReflection):
    __abstract__ = True
    
    
def create_proteins_table(version:int):

    # class Base(DeclarativeBase):
    #     pass 

    # class Reflected(DeferredReflection):
    #     __abstract__ = True
        

    parents = (Base, Reflected)
    name = f'Proteins_r{version}'
    
    attrs = dict()
    attrs['__tablename__'] = f'proteins_r{version}'
    attrs['__table_args__'] = (ForeignKeyConstraint(['genome_id'], [f'metadata_r{version}.genome_id']),
                                {'extend_existing':True})

    # Set table column attributes. 
    attrs['gene_id'] = mapped_column(String(GENE_ID_LENGTH), primary_key=True)
    attrs['version'] = mapped_column(Integer, comment='The GTDB version from which the data was obtained.')
    attrs['genome_id'] = mapped_column(String(GENOME_ID_LENGTH), comment='The GTDB genome ID.')
    # seq = mapped_column(String(MAX_SEQ_LENGTH), comment='The amino acid sequence.')
    # nt_seq = mapped_column(LONGTEXT, comment='The nucleotide acid sequence.')
    attrs['aa_seq'] = mapped_column(Text, comment='The amino acid sequence.')
    attrs['start'] = mapped_column(Integer, comment='The start location of the gene in the genome.')
    attrs['stop'] = mapped_column(Integer, comment='The stop location of the gene in the genome.')
    attrs['start_codon'] = mapped_column(String(3), comment='The start codon of the sequence.')
    attrs['stop_codon'] = mapped_column(String(3), comment='The stop codon of the sequence.')
    attrs['gc_content'] = mapped_column(Float) # The GC content of the gene. 
    attrs['strand' ]= mapped_column(CHAR) # Whether the gene is on the forward of reverse strand. 
    attrs['partial'] = mapped_column(String(2), comment='An indicator of if a gene runs off the edge of a sequence or into a gap. A 0 indicates the gene has a true boundary (a start or a stop), whereas a 1 indicates the gene is partial at that edge. For example, 00 indicates a complete gene with a start and stop codon.') 
    attrs['rbs_motif'] = mapped_column(String(DEFAULT_STRING_LENGTH)) # The RBS binding motif detected by Prodigal. 
    attrs['scaffold_id'] = mapped_column(Integer) # TODO: How do I extract this?

    return type(name, parents, attrs)


def create_annotations_kegg_table(version:int):

    # class Base(DeclarativeBase):
    #     pass 

    # class Reflected(DeferredReflection):
    #     __abstract__ = True

    name = f'AnnotationsKegg_r{version}'
    parents = (Base, Reflected)

    attrs = dict()
    attrs['__tablename__'] = f'annotations_kegg_r{version}'
    attrs[f'metadata_r{version}'] = relationship(f'Metadata_r{version}', viewonly=True)
    attrs[f'proteins_r{version}'] = relationship(f'Proteins_r{version}', viewonly=True)
    attrs['__table_args__'] = (ForeignKeyConstraint(['genome_id'], [f'metadata_r{version}.genome_id']), 
                                ForeignKeyConstraint(['gene_id'], [f'proteins_r{version}.gene_id']),
                                {'extend_existing':True})  

    # Set table column attributes. 
    attrs['annotation_id'] = mapped_column(Integer, primary_key=True)
    attrs['version'] = mapped_column(Integer, comment='The GTDB version from which the data was obtained.')
    attrs['gene_id'] = mapped_column(String(GENE_ID_LENGTH))
    attrs['genome_id'] = mapped_column(String(GENOME_ID_LENGTH))
    attrs['ko'] = mapped_column(String(DEFAULT_STRING_LENGTH), index=True) # The KEGG Orthology group with which the gene was annotated.
    attrs['threshold'] = mapped_column(Float) # The adaptive threshold for the bitscore generated using Kofamscan
    attrs['score'] = mapped_column(Float) # The bit score generated using Kofamscan which gives a measure of similarity between the gene and the KO family.
    attrs['e_value'] = mapped_column(Float)

    return type(name, parents, attrs)


def create_metadata_table(version:int):

    # class Base(DeclarativeBase):
    #     pass 

    # class Reflected(DeferredReflection):
    #     __abstract__ = True

    name = f'Metadata_r{version}'
    parents = (Base, Reflected)

    attrs = dict()
    attrs['__tablename__'] = f'metadata_r{version}'
    attrs['__table_args__'] = {'extend_existing':True}

    # Set attributes for table columns.
    attrs['genome_id'] = mapped_column(String(GENOME_ID_LENGTH), primary_key=True)
    attrs['version'] = mapped_column(Integer, comment='The GTDB version from which the data was obtained.')
    attrs['gtdb_order'] = mapped_column(String(DEFAULT_STRING_LENGTH))
    attrs['gtdb_domain'] = mapped_column(String(DEFAULT_STRING_LENGTH))
    attrs['gtdb_phylum'] = mapped_column(String(DEFAULT_STRING_LENGTH))
    attrs['gtdb_class'] = mapped_column(String(DEFAULT_STRING_LENGTH))
    attrs['gtdb_family'] = mapped_column(String(DEFAULT_STRING_LENGTH))
    attrs['gtdb_genus'] = mapped_column(String(DEFAULT_STRING_LENGTH))
    attrs['gtdb_species'] = mapped_column(String(DEFAULT_STRING_LENGTH))
    attrs['checkm_completeness'] = mapped_column(Float)
    attrs['checkm_contamination'] = mapped_column(Float)
    attrs['coding_bases'] = mapped_column(Integer)
    attrs['coding_density'] = mapped_column(Float)
    attrs['contig_count'] = mapped_column(Integer)
    attrs['gc_content'] = mapped_column(Float)
    attrs['genome_size'] = mapped_column(Float)
    attrs['l50_contigs'] = mapped_column(Integer)
    attrs['l50_scaffolds'] = mapped_column(Integer)
    attrs['n50_contigs'] = mapped_column(Integer)
    attrs['n50_scaffolds'] = mapped_column(Integer)
    attrs['longest_contig'] = mapped_column(Integer)
    attrs['longest_scaffold'] = mapped_column(Integer)
    attrs['mean_contig_length'] = mapped_column(Float)
    attrs['sec_trna_count'] = mapped_column(Integer)
    attrs['mean_scaffold_length'] = mapped_column(Float)
    attrs['protein_count'] = mapped_column(Integer)
    attrs['ncbi_genome_representation'] = mapped_column(String(DEFAULT_STRING_LENGTH))
    
    return type(name, parents, attrs)



def create_annotations_pfam_table(version:int):

    # class Base(DeclarativeBase):
    #     pass 

    # class Reflected(DeferredReflection):
    #     __abstract__ = True

    name = f'AnnotationsPfam_r{version}'
    parents = (Base, Reflected)

    attrs = dict()
    attrs['__tablename__'] = f'annotations_pfam_r{version}'
    attrs[f'metadata_r{version}'] = relationship(f'Metadata_r{version}', viewonly=True)
    attrs[f'proteins_r{version}'] = relationship(f'Proteins_r{version}', viewonly=True)
    attrs['__table_args__'] = (ForeignKeyConstraint(['genome_id'], [f'metadata_r{version}.genome_id']), 
                                ForeignKeyConstraint(['gene_id'], [f'proteins_r{version}.gene_id']),
                                {'extend_existing':True}) 

    # Set attributes for all the table columns. 
    attrs['annotation_id'] = mapped_column(Integer, primary_key=True)
    attrs['version'] = mapped_column(Integer, comment='The GTDB version from which the data was obtained.')
    attrs['gene_id'] = mapped_column(String(GENE_ID_LENGTH))
    attrs['genome_id'] = mapped_column(String(GENOME_ID_LENGTH))
    attrs['pfam'] = mapped_column(String(DEFAULT_STRING_LENGTH), index=True) # The KEGG Orthology group with which the gene was annotated.
    attrs['start'] = mapped_column(Integer)
    attrs['stop'] = mapped_column(Integer)
    attrs['length'] = mapped_column(Integer)
    attrs['e_value'] = mapped_column(Float)
    attrs['interpro_accession'] = mapped_column(String(DEFAULT_STRING_LENGTH))
    attrs['interpro_description'] = mapped_column(String(200))

    return type(name, parents, attrs) 


