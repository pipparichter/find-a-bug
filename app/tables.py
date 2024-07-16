'''Setting up ORM for the Find-A-Bug database. Reflects SQL tables which already exist, as created by scripts in the data subdirectory.'''
import os
from sqlalchemy import inspect
import sqlalchemy
import sqlalchemy.orm
from releaseed import releaseed
from sqlalchemy import String, Integer, ForeignKey, PrimaryKeyConstraint, Float # , ForeignKeyConstraint
from sqlalchemy.orm import DeclarativeBase, relationship, mapped_column
from sqlalchemy.ext.declarative import DeferredReflection
from typing import List, Dict, Set

# TODO: Read https://www.geeksforgeeks.org/sqlalchemy-orm-declaring-mapping/

# NOTE: Useful documentation for managing relationships here: https://docs.sqlalchemy.org/en/20/orm/relationship_api.html

# NOTE: The back_populates parameter indicates the name of a relationship() on the related class that will be synchronized 
# with this one. It is usually expected that the relationship() on the related class also refer to this one. This allows objects 
# on both sides of each relationship() to synchronize state changes.

# NOTE: Because the relationship from amino acid sequences to annotations should be one-to-one, should specify uselist=False.
# NOTE: The viewonly=True specifies that the relationship should not be used for persistence operations, only for accessing values.

# NOTE: I think I am going to make the design choice to have the Proteins table as the primary table, and access everything else from there. 
# There should always be a many-to-one (proteins to metadata) or one-to-many (proteins to annotations) relationship.


class Base(DeclarativeBase):
    pass


class Reflected(DeferredReflection):
    __abstract__ = True


class ProteinsBase(Reflected, Base):
    __abstract__ = True

    gene_id = mapped_column(String, primary_key=True)
    release = mapped_column(Integer)
    genome_id = mapped_column(String)
    
    seq = mapped_column(String, comment='The amino acid sequence.')
    start = mapped_column(Integer, comment='The start location of the gene in the genome.')
    stop = mapped_column(Integer, comment='The stop location of the gene in the genome.')
    gc_content = mapped_column(Float) # The GC content of the gene. 
    strand = mapped_column(String(1)) # Whether the gene is on the forward of reverse strand. 
    start_type = mapped_column(String) # Either a codon or indicates that the gene is incomplete. 
    partial = mapped_column(String) # Indicating if the gene is partial, i.e. runs off a contig. 
    rbs_motif = mapped_column(String) # The RBS binding motif detected by Prodigal. 
    rbs_spacer = mapped_column(String) # The RBS spacer detected by Prodigal. 
    scaffold_id = mapped_column(String) # TODO: How do I extract this?


# Should I combine PFAM and KEGG annotations in the same table?
class AnnotationsKeggBase(Reflected, Base):
    __abstract__ = True
 
    annotation_id = mapped_column(String, primary_key=True)
    release = mapped_column(String)
    gene_id = mapped_column(String)
    genome_id = mapped_column(String)

    ko = mapped_column(String) # The KEGG Orthology group with which the gene was annotated.
    threshold = mapped_column(Float) # The adaptive threshold for the bitscore generated using Kofamscan
    score = mapped_column(Float) # The bit score generated using Kofamscan which gives a measure of similarity between the gene and the KO family.
    e_value = mapped_column(Float)


class AnnotationsPfamBase(Reflected, Base):
    __abstract__ = True

    annotation_id = mapped_column(String, primary_key=True)
    release = mapped_column(String)
    gene_id = mapped_column(String)
    genome_id = mapped_column(String)

    pfam = mapped_column(String) # The KEGG Orthology group with which the gene was annotated.
    start = mapped_column(Integer)
    stop = mapped_column(Integer)
    length = mapped_column(Integer)
    e_value = mapped_column(Float)
    interpro_accession = mapped_column(String)
    interpro_description = mapped_column(String)
 

class MetadataBase(Reflected, Base):
    __abstract__ = True 

    genome_id = mapped_column(String, primary_key=True)
    
    gtdb_order = mapped_column(String)
    gtdb_domain = mapped_column(String)
    gtdb_phylum = mapped_column(String)
    gtdb_class = mapped_column(String)
    gtdb_family = mapped_column(String)
    gtdb_genus = mapped_column(String)
    gtdb_species = mapped_column(String)
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
    ncbi_genome_representation = mapped_column(String)



class MetadataHistory(MetadataBase):
    __tablename__ = 'metadata_history'
    __table_args__ = (PrimaryKeyConstraint('genome_id', 'release', name=__tablename__))

    proteins_history = relationship('AminoAcidSeqsHistory', back_populates='metadata_history')
    annotations_kegg_history = relationship('AnnotationsKeggHistory', back_populates='metadata_history')
    annotations_pfam_history = relationship('AnnotationsPfamHistory', back_populates='metadata_history')



class ProteinsHistory(ProteinsBase):
    __tablename__ = 'proteins_history'
    __table_args__ = (PrimaryKeyConstraint('gene_id', 'release', name=__tablename__))

    metadata_history = relationship('MetadataHistory', foreign_keys=['metadata.genome_id', 'metadata.release'], back_populates='proteins_history')
    annotations_kegg_history = relationship('AnnotationsKeggHistory', back_populates='proteins_history')
    annotations_pfam_history = relationship('AnnotationsPfamHistory', back_populates='proteins_history')


class AnnotationsKeggHistory(AnnotationsKeggBase):
    __tablename__ = 'annotations_kegg_history'
    __table_args__ = (PrimaryKeyConstraint('annotation_id', 'release', name=__tablename__))

    proteins_history = relationship('ProteinsHistory', foreign_keys=['proteins_history.gene_id', 'proteins_history.release'], back_populates='annotations_kegg_history')
    metadata_history = relationship('MetadataHistory', foreign_keys=['metadata_history.genome_id', 'metadata_history.release'], back_populates='annotations_kegg_history')
    


class AnnotationsPfamHistory(AnnotationsPfamBase):
    __tablename__ = 'annotations_pfam_history'
    __table_args__ = (PrimaryKeyConstraint('annotation_id', 'release', name=__tablename__))
   
    proteins_history = relationship('ProteinsHistory', foreign_keys=['proteins_history.gene_id', 'proteins_history.release'], back_populates='annotations_pfam_history')
    metadata_history = relationship('MetadataHistory', foreign_keys=['metadata_history.genome_id', 'metadata_history.release'], back_populates='annotations_pfam_history')
    

class Metadata(MetadataBase):
    __tablename__ = 'metadata'
    
    proteins = relationship('Proteins', back_populates='metadata')
    annotations_kegg = relationship('AnnotationsKegg', back_populates='metadata')
    annotations_kegg = relationship('AnnotationsPfam', back_populates='metadata')


class Proteins(ProteinsBase):
    __tablename__ = 'proteins'

    metadata = relationship('Metadata', back_populates='proteins', foreign_keys=['proteins.gene_id'])
    annotations_kegg = relationship('AnnotationsKegg', back_populates='proteins')
    annotations_pfam = relationship('AnnotationsPfam', back_populates='proteins')


class AnnotationsKegg(AnnotationsKeggBase):
    __tablename__ = 'annotations_kegg'

    proteins = relationship('Proteins', back_populates='annotations_kegg')
    metadata = relationship('Metadata', back_populates='annotations_kegg')


class AnnotationsPfam(AnnotationsPfamBase):
    __tablename__ = 'annotations_pfam'

    proteins = relationship('Proteins', back_populates='annotations_pfam', foreign_keys=['proteins.gene_id'])
    metadata = relationship('Metadata', back_populates='annotations_pfam', foreign_keys=['metadata.genome_id'])


# class Database():
#     '''The class which mediates the interactions between the database and Flask app.'''

#     def __init__(self, engine):
        
#         # First need to reflect the current database into Table objects.
#         Reflected.prepare(engine)
#         self.tables = [Metadata, AminoAcidSeqs, AnnotationsKegg, AnnotationsPfam, 
#             MetadataHistory, AminoAcidSeqsHistory, AnnotationsKeggHistory, AnnotationsPfamHistory]
    
#     def get_field_to_table_map(self, fields:Set[str], table:sqlalchemy.Table=None) -> Dict[str, sqlalchemy.Table]:
#         '''Returns a dictionary mapping the fields in the input to the SQLAlchemy table in which
#         they are found. Minimizes the number of tables required to cover the fields.
        
#         :param fields: The fields which must be covered in the set of tables returned by this function. 
#         :param table: The main query table, i.e. the one which corresponds to the requested resource. 
#         :return: A dictionary mapping each input field to a table which contains it. 
#         '''
#         # Initialize the field_to_table_map with the fields in the main table. 
#         # This will hopefully avoid any unnecessary joins. 
#         field_to_table_map = {f:table for f in fields.intersection(self.get_fields(table))}
#         fields = fields - self.get_fields(table) # Remove the fields which have already been added. 

#         # Sort the tables according to the number of columns they "cover." 
#         all_tables = sorted(self.tables, key=lambda t: len(t.__table__.c))
#         all_tables.remove(table) # Exclude the main query table. 
#         for t in all_tables:
#             if len(fields) == 0:
#                 break
#             field_to_table_map.update({f:t for f in fields.intersection(self.get_fields(t))})
#             fields = fields - self.get_fields(t)

#         assert len(fields) == 0, f'database.get_field_to_table_map: The fields {fields} were not mapped to a table.'
#         return field_to_table_map

#     def get_fields(self, table:sqlalchemy.Table) -> Set[str]:
#         '''Get all the fields stored in the table.
        
#         :param table: A SQLAlchemy table for which to grab the field names. 
#         :return: A set of strings representing the columns of the table. 
#         '''
#         return set([c.name for c in table.__table__.c])

#     def get_table(self, name:str) -> sqlalchemy.Table:
#         '''Get the table corresponding to the given name.
        
#         :param name: The name of the table to grab.
#         :return: The SQLAlchemy table object with the specified name. 
#         :raise: ValueError if the input table name does not match any in the database. 
#         '''
#         # Probably should treat this like a dictionary, eventually. 
#         for table in self.tables:
#             if table.__tablename__ == name:
#                 return table

#         raise ValueError('database.FindABugDatabase.get_table: Table {name} is not present in the database.')
   
