import os
from sqlalchemy import inspect
import sqlalchemy
import pandas as pd 
import numpy as np
import sqlalchemy.orm
from sqlalchemy import String, Integer, ForeignKey, PrimaryKeyConstraint, Float, ForeignKeyConstraint
from sqlalchemy.orm import DeclarativeBase, relationship, mapped_column
from sqlalchemy.ext.declarative import DeferredReflection
from typing import List, Dict, Set
from utils import MAX_GENE_LENGTH, GENOME_ID_LENGTH, GENE_ID_LENGTH, DEFAULT_STRING_LENGTH


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


class ProteinsBase(Reflected, Base):
    __abstract__ = True

    gene_id = mapped_column(String(GENE_ID_LENGTH), primary_key=True)
    gtdb_version = mapped_column(Integer, comment='The GTDB gtdb_version from which the data was obtained.')
    genome_id = mapped_column(String(GENOME_ID_LENGTH))
    
    seq = mapped_column(String(MAX_GENE_LENGTH), comment='The amino acid sequence.')
    start = mapped_column(Integer, comment='The start location of the gene in the genome.')
    stop = mapped_column(Integer, comment='The stop location of the gene in the genome.')
    gc_content = mapped_column(Float) # The GC content of the gene. 
    strand = mapped_column(String(1)) # Whether the gene is on the forward of reverse strand. 
    start_type = mapped_column(String(5), comment='The sequence of the start codon. If the gene has no start codon, this field will be labeled "Edge."')
    partial = mapped_column(String(2), comment='An indicator of if a gene runs off the edge of a sequence or into a gap. A 0 indicates the gene has a true boundary (a start or a stop), whereas a 1 indicates the gene is partial at that edge. For example, 00 indicates a complete gene with a start and stop codon.') 
    rbs_motif = mapped_column(String(DEFAULT_STRING_LENGTH)) # The RBS binding motif detected by Prodigal. 
    rbs_spacer = mapped_column(String(DEFAULT_STRING_LENGTH)) # The RBS spacer detected by Prodigal. 
    scaffold_id = mapped_column(String(DEFAULT_STRING_LENGTH)) # TODO: How do I extract this?


# Should I combine PFAM and KEGG annotations in the same table?
class AnnotationsKeggBase(Reflected, Base):
    __abstract__ = True
 
    annotation_id = mapped_column(Integer, primary_key=True)
    gtdb_version = mapped_column(Integer, comment='The GTDB gtdb_version from which the data was obtained.')
    gene_id = mapped_column(String(GENE_ID_LENGTH))
    genome_id = mapped_column(String(GENOME_ID_LENGTH))

    ko = mapped_column(String(DEFAULT_STRING_LENGTH)) # The KEGG Orthology group with which the gene was annotated.
    threshold = mapped_column(Float) # The adaptive threshold for the bitscore generated using Kofamscan
    score = mapped_column(Float) # The bit score generated using Kofamscan which gives a measure of similarity between the gene and the KO family.
    e_value = mapped_column(Float)


class AnnotationsPfamBase(Reflected, Base):
    __abstract__ = True

    annotation_id = mapped_column(Integer, primary_key=True)
    gtdb_version = mapped_column(Integer, comment='The GTDB gtdb_version from which the data was obtained.')
    gene_id = mapped_column(String(GENE_ID_LENGTH))
    genome_id = mapped_column(String(GENOME_ID_LENGTH))

    pfam = mapped_column(String(DEFAULT_STRING_LENGTH)) # The KEGG Orthology group with which the gene was annotated.
    start = mapped_column(Integer)
    stop = mapped_column(Integer)
    length = mapped_column(Integer)
    e_value = mapped_column(Float)
    interpro_accession = mapped_column(String(DEFAULT_STRING_LENGTH))
    interpro_description = mapped_column(String(DEFAULT_STRING_LENGTH))
 

class MetadataBase(Reflected, Base):
    __abstract__ = True 

    genome_id = mapped_column(String(GENOME_ID_LENGTH), primary_key=True)
    gtdb_version = mapped_column(Integer, comment='The GTDB gtdb_version from which the data was obtained.')

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
    mean_scaffold_length = mapped_column(Float)
    ncbi_genome_representation = mapped_column(String(DEFAULT_STRING_LENGTH))



class MetadataHistory(MetadataBase):
    __tablename__ = 'metadata_history'
    __table_args__ = (PrimaryKeyConstraint('genome_id', 'gtdb_version', name=__tablename__),)


    proteins_history = relationship('ProteinsHistory', viewonly=True) # , passive_deletes=True)
    annotations_kegg_history = relationship('AnnotationsKeggHistory', viewonly=True) # , passive_deletes=True)
    annotations_pfam_history = relationship('AnnotationsPfamHistory', viewonly=True) # , passive_deletes=True)



class ProteinsHistory(ProteinsBase):
    __tablename__ = 'proteins_history'
    __table_args__ = (PrimaryKeyConstraint('gene_id', 'gtdb_version', name=__tablename__),
                        ForeignKeyConstraint(['genome_id', 'gtdb_version'], ['metadata_history.genome_id', 'metadata_history.gtdb_version'])) # , ondelete='cascade'))

    annotations_kegg_history = relationship('AnnotationsKeggHistory', viewonly=True) # , passive_deletes=True)
    annotations_pfam_history = relationship('AnnotationsPfamHistory', viewonly=True) # , passive_deletes=True)


class AnnotationsKeggHistory(AnnotationsKeggBase):
    __tablename__ = 'annotations_kegg_history'
    __table_args__ = (PrimaryKeyConstraint('annotation_id', 'gtdb_version', name=__tablename__),
                        ForeignKeyConstraint(['genome_id', 'gtdb_version'], ['metadata_history.genome_id', 'metadata_history.gtdb_version']), # , ondelete='cascade'),
                        ForeignKeyConstraint(['gene_id', 'gtdb_version'], ['proteins_history.gene_id', 'proteins_history.gtdb_version'])) # , ondelete='cascade'))


class AnnotationsPfamHistory(AnnotationsPfamBase):
    __tablename__ = 'annotations_pfam_history'
    __table_args__ = (PrimaryKeyConstraint('annotation_id', 'gtdb_version', name=__tablename__),
                        ForeignKeyConstraint(['genome_id', 'gtdb_version'], ['metadata_history.genome_id', 'metadata_history.gtdb_version']), # , ondelete='cascade'),
                        ForeignKeyConstraint(['gene_id', 'gtdb_version'], ['proteins_history.gene_id', 'proteins_history.gtdb_version'])) # , ondelete='cascade')) 


class Metadata(MetadataBase):
    __tablename__ = 'metadata'
    
    proteins_history = relationship('Proteins', viewonly=True) # , passive_deletes=True) 
    annotations_kegg_history = relationship('AnnotationsKegg', viewonly=True) # , passive_deletes=True) 
    annotations_pfam_history = relationship('AnnotationsPfam', viewonly=True) # , passive_deletes=True) 


class Proteins(ProteinsBase):
    __tablename__ = 'proteins'
    __table_args__ = (ForeignKeyConstraint(['genome_id'], ['metadata.genome_id'])) # , ondelete='cascade'),)

    annotations_kegg_history = relationship('AnnotationsKegg', viewonly=True) # , passive_deletes=True)
    annotations_pfam_history = relationship('AnnotationsPfam', viewonly=True) # , passive_deletes=True)


class AnnotationsKegg(AnnotationsKeggBase):
    __tablename__ = 'annotations_kegg'
    __table_args__ = (ForeignKeyConstraint(['genome_id'], ['metadata.genome_id']), # , ondelete='cascade'),
                        ForeignKeyConstraint(['gene_id'], ['proteins.gene_id'])) # , ondelete='cascade')) 


class AnnotationsPfam(AnnotationsPfamBase):
    __tablename__ = 'annotations_pfam'
    __table_args__ = (ForeignKeyConstraint(['genome_id'], ['metadata.genome_id']), # , ondelete='cascade'),
                        ForeignKeyConstraint(['gene_id'], ['proteins.gene_id'])) # , ondelete='cascade')) 

