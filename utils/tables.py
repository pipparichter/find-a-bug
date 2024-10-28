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


class ProteinsBase(Base):
    __abstract__ = True

    gene_id = mapped_column(String(GENE_ID_LENGTH), primary_key=True)
    version = mapped_column(Integer, comment='The GTDB version from which the data was obtained.')
    genome_id = mapped_column(String(GENOME_ID_LENGTH), comment='The GTDB genome ID.')
    
    # seq = mapped_column(String(MAX_SEQ_LENGTH), comment='The amino acid sequence.')
    aa_seq = mapped_column(Text, comment='The amino acid sequence.')
    # nt_seq = mapped_column(LONGTEXT, comment='The nucleotide acid sequence.')
    start = mapped_column(Integer, comment='The start location of the gene in the genome.')
    stop = mapped_column(Integer, comment='The stop location of the gene in the genome.')
    start_codon = mapped_column(String(3), comment='The start codon of the sequence.')
    stop_codon = mapped_column(String(3), comment='The stop codon of the sequence.')
    gc_content = mapped_column(Float) # The GC content of the gene. 
    strand = mapped_column(CHAR) # Whether the gene is on the forward of reverse strand. 
    partial = mapped_column(String(2), comment='An indicator of if a gene runs off the edge of a sequence or into a gap. A 0 indicates the gene has a true boundary (a start or a stop), whereas a 1 indicates the gene is partial at that edge. For example, 00 indicates a complete gene with a start and stop codon.') 
    rbs_motif = mapped_column(String(DEFAULT_STRING_LENGTH)) # The RBS binding motif detected by Prodigal. 
    scaffold_id = mapped_column(Integer) # TODO: How do I extract this?


# Should I combine PFAM and KEGG annotations in the same table?
class AnnotationsKeggBase(Base):
    __abstract__ = True
 
    annotation_id = mapped_column(Integer, primary_key=True)
    version = mapped_column(Integer, comment='The GTDB version from which the data was obtained.')
    gene_id = mapped_column(String(GENE_ID_LENGTH))
    genome_id = mapped_column(String(GENOME_ID_LENGTH))

    ko = mapped_column(String(DEFAULT_STRING_LENGTH), index=True) # The KEGG Orthology group with which the gene was annotated.
    threshold = mapped_column(Float) # The adaptive threshold for the bitscore generated using Kofamscan
    score = mapped_column(Float) # The bit score generated using Kofamscan which gives a measure of similarity between the gene and the KO family.
    e_value = mapped_column(Float)


class AnnotationsPfamBase(Base):
    __abstract__ = True

    annotation_id = mapped_column(Integer, primary_key=True)
    version = mapped_column(Integer, comment='The GTDB version from which the data was obtained.')
    gene_id = mapped_column(String(GENE_ID_LENGTH))
    genome_id = mapped_column(String(GENOME_ID_LENGTH))

    pfam = mapped_column(String(DEFAULT_STRING_LENGTH), index=True) # The KEGG Orthology group with which the gene was annotated.
    start = mapped_column(Integer)
    stop = mapped_column(Integer)
    length = mapped_column(Integer)
    e_value = mapped_column(Float)
    interpro_accession = mapped_column(String(DEFAULT_STRING_LENGTH))
    interpro_description = mapped_column(String(200))
 

class MetadataBase(Base):
    __abstract__ = True 

    genome_id = mapped_column(String(GENOME_ID_LENGTH), primary_key=True)
    version = mapped_column(Integer, comment='The GTDB version from which the data was obtained.')

    gtdb_order = mapped_column(String(DEFAULT_STRING_LENGTH))
    gtdb_domain = mapped_column(String(DEFAULT_STRING_LENGTH))
    gtdb_phylum = mapped_column(String(DEFAULT_STRING_LENGTH))
    gtdb_class = mapped_column(String(DEFAULT_STRING_LENGTH))
    gtdb_family = mapped_column(String(DEFAULT_STRING_LENGTH))
    gtdb_genus = mapped_column(String(DEFAULT_STRING_LENGTH))
    gtdb_species = mapped_column(String(DEFAULT_STRING_LENGTH))
    checkm_completeness = mapped_column(Float)
    checkm_contamination = mapped_column(Float)
    coding_bases = mapped_column(Integer)
    coding_density = mapped_column(Float)
    contig_count = mapped_column(Integer)
    gc_content = mapped_column(Float)
    genome_size = mapped_column(Float)
    l50_contigs = mapped_column(Integer)
    l50_scaffolds = mapped_column(Integer)
    n50_contigs = mapped_column(Integer)
    n50_scaffolds = mapped_column(Integer)
    longest_contig = mapped_column(Integer)
    longest_scaffold = mapped_column(Integer)
    mean_contig_length = mapped_column(Float)
    sec_trna_count = mapped_column(Integer)
    mean_scaffold_length = mapped_column(Float)
    protein_count = mapped_column(Integer)
    ncbi_genome_representation = mapped_column(String(DEFAULT_STRING_LENGTH))


def create_metadata_table(version:int):
    name = f'Metadata_r{version}'
    parents = (MetadataBase, Reflected)

    attrs = dict()
    attrs['__tablename__'] = f'metadata_r{version}'
    attrs['__table_args__'] = {'extend_existing':True}
    
    return type(name, parents, attrs)


def create_proteins_table(version:int):
    name = f'Proteins_r{version}'
    parents = (ProteinsBase, Reflected)

    attrs = dict()
    attrs['__tablename__'] = f'proteins_r{version}'
    attrs['metadata_'] = relationship(f'Metadata_r{version}', viewonly=True)
    attrs['__table_args__'] = (ForeignKeyConstraint(['genome_id'], [f'metadata_r{version}.genome_id']),
                            {'extend_existing':True})
    
    return type(name, parents, attrs)


def create_annotations_kegg_table(version:int):
    name = f'AnnotationsKegg_r{version}'
    parents = (AnnotationsKeggBase, Reflected)

    attrs = dict()
    attrs['__tablename__'] = f'annotations_kegg_r{version}'
    attrs['metadata_'] = relationship(f'Metadata_r{version}', viewonly=True)
    attrs['proteins'] = relationship(f'Proteins_r{version}', viewonly=True)
    attrs['__table_args__'] = (ForeignKeyConstraint(['genome_id'], [f'metadata_r{version}.genome_id']), 
                                ForeignKeyConstraint(['gene_id'], [f'proteins_r{version}.gene_id']),
                                {'extend_existing':True})  

    return type(name, parents, attrs)


def create_annotations_pfam_table(version:int):
    name = f'AnnotationsPfam_r{version}'
    parents = (AnnotationsPfamBase, Reflected)

    attrs = dict()
    attrs['__tablename__'] = f'annotations_pfam_r{version}'
    attrs['metadata_'] = relationship(f'Metadata_r{version}', viewonly=True)
    attrs['proteins'] = relationship(f'Proteins_r{version}', viewonly=True)
    attrs['__table_args__'] = (ForeignKeyConstraint(['genome_id'], [f'metadata_r{version}.genome_id']), 
                                ForeignKeyConstraint(['gene_id'], [f'proteins_r{version}.gene_id']), 
                                {'extend_existing':True}) 

    return type(name, parents, attrs) 



# class Metadata(MetadataBase, Reflected):
#     __tablename__ = 'metadata'
    
#     # proteins_history = relationship('Proteins', viewonly=True) # , passive_deletes=True) 
#     # annotations_kegg_history = relationship('AnnotationsKegg', viewonly=True) # , passive_deletes=True) 
#     # annotations_pfam_history = relationship('AnnotationsPfam', viewonly=True) # , passive_deletes=True) 


# class Proteins(ProteinsBase, Reflected):
#     __tablename__ = 'proteins'
#     __table_args__ = (ForeignKeyConstraint(['genome_id'], ['metadata.genome_id']),) # , ondelete='cascade'),)

#     metadata_ = relationship('Metadata', viewonly=True)


# class AnnotationsKegg(AnnotationsKeggBase, Reflected):
#     __tablename__ = 'annotations_kegg'
#     __table_args__ = (ForeignKeyConstraint(['genome_id'], ['metadata.genome_id']), # , ondelete='cascade'),
#                         ForeignKeyConstraint(['gene_id'], ['proteins.gene_id'])) # , ondelete='cascade')) 

#     metadata_ = relationship('Metadata', viewonly=True)
#     proteins = relationship('Proteins', viewonly=True)


# class AnnotationsPfam(AnnotationsPfamBase, Reflected):
#     __tablename__ = 'annotations_pfam'
#     __table_args__ = (ForeignKeyConstraint(['genome_id'], ['metadata.genome_id']), # , ondelete='cascade'),
#                         ForeignKeyConstraint(['gene_id'], ['proteins.gene_id'])) # , ondelete='cascade')) 

#     metadata_ = relationship('Metadata', viewonly=True)
#     proteins = relationship('Proteins', viewonly=True)
