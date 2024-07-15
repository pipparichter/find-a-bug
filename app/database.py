'''Setting up ORM for the Find-A-Bug database. Reflects SQL tables which already exist, as created by scripts in the data subdirectory.'''
import os
from sqlalchemy import inspect
import sqlalchemy
import sqlalchemy.orm
from releaseed import releaseed
from sqlalchemy import String, Integer, ForeignKey, PrimaryKeyConstraint # , ForeignKeyConstraint
from sqlalchemy.orm import DeclarativeBase, relationship, mapped_column
from sqlalchemy.ext.declarative import DeferredReflection
from typing import List, Dict, Set

# TODO: Read https://www.geeksforgeeks.org/sqlalchemy-orm-declaring-mapping/

# NOTE: Useful documentation for managing relationships here: https://docs.sqlalchemy.org/en/20/orm/relationship_api.html

class Base(DeclarativeBase):
    pass


# TODO: Need a refresher on what the declarative mapping is doing. 
class Reflected(DeferredReflection):
    '''To accommodate the use case of declaring mapped classes where reflection of table metadata can occur afterwards. 
    It alters the declarative mapping process to be delayed, and will integrate the results with the declarative table
    mapping process.'''
    __abstract__ = True # NOTE: What does this mean?


class MetadataHistory(Reflected, Base):
    __tablename__ = 'gtdb_metadata_history'

    genome_id = mapped_column(String, primary_key=True)
    release = mapped_column(Integer, primary_key=True)
    
    __table_args__ = (PrimaryKeyConstraint('genome_id', 'release', name=__tablename__))

    # Need to think about how to configure the relationships. Do I want each table pointing back to its histories, or
    # all of the histories interacting with each other? 
    gtdb_amino_acid_seqs_history = relationship('AminoAcidSeqsHistory', foreign_keys=[genome_id, release])
    gtdb_annotations_kegg_history = relationship('AnnotationsKeggHistory', foreign_keys=[genome_id, release])
    gtdb_annotations_pfam_history = relationship('AnnotationsPfamHistory', foreign_keys=[genome_id, release])


class AminoAcidSeqsHistory(Reflected, Base):
    __tablename__ = 'gtdb_amino_acid_seqs_history'

    gene_id = mapped_column(String, primary_key=True)
    release = mapped_column(String, primary_key=True)
    genome_id = mapped_column(String, ForeignKey('gtdb_metadata_history.genome_id'))

    __table_args__ = (PrimaryKeyConstraint('gene_id', 'release', name=__tablename__))
    
    
    gtdb_metadata_history = relationship('MetadataHistory', viewonly=True, foreign_keys=[genome_id, release])
    gtdb_annotations_kegg_history = relationship('AnnotationsKeggHistory', foreign_keys=[gene_id, release], back_populates='gtdb_amino_acid_seqs_history')
    gtdb_annotations_pfam_history = relationship('AnnotationsPfamHistory', foreign_keys=[gene_id, release], back_populates='gtdb_amino_acid_seqs_history')


class AnnotationsKeggHistory(Reflected, Base):
    __tablename__ = 'gtdb_annotations_kegg_history'
    
    annotation_id = mapped_column(String, primary_key=True)
    release = mapped_column(String, primary_key=True)
    gene_id = mapped_column(String, ForeignKey('gtdb_amino_acid_seqs_history.gene_id'))
    genome_id = mapped_column(String, ForeignKey('gtdb_metadata_history.genome_id'))

    __table_args__ = (PrimaryKeyConstraint('annotation_id', 'release', name=__tablename__))
    
    gtdb_amino_acid_seqs_history = relationship('AminoAcidSeqsHistory', foreign_keys=[gene_id, release], back_populates='gtdb_annotations_kegg_history', uselist=False)
    gtdb_metadata_history = relationship('MetadataHistory', foreign_keys=[genome_id, release], viewonly=True)


class AnnotationsPfamHistory(Reflected, Base):
    __tablename__ = 'gtdb_annotations_pfam_history'
    
    annotation_id = mapped_column(String, primary_key=True) # These annotation IDs are different than those in the KEGG annotation table. 
    release = mapped_column(String, primary_key=True)
    gene_id = mapped_column(String, ForeignKey('gtdb_amino_acid_seqs_history.gene_id'))
    genome_id = mapped_column(String, ForeignKey('gtdb_metadata.genome_id'))

    __table_args__ = (PrimaryKeyConstraint('annotation_id', 'release', name=__tablename__))

    # NOTE: The back_populates parameter indicates the name of a relationship() on the related class that will be synchronized 
    # with this one. It is usually expected that the relationship() on the related class also refer to this one. This allows objects 
    # on both sides of each relationship() to synchronize state changes.

    # NOTE: Because the relationship from amino acid sequences to annotations should be one-to-one, should specify uselist=False.
    # NOTE: The viewonly=True specifies that the relationship should not be used for persistence operations, only for accessing values.
    gtdb_amino_acid_seqs_history = relationship('AminoAcidSeqsHistory', foreign_keys=[gene_id, release], back_populates='gtdb_annotations_pfam_history', uselist=False)
    gtdb_metadata_history = relationship('MetadataHistory', foreign_keys=[genome_id, release], viewonly=True)
    


class Metadata(Reflected, Base):

    __tablename__ = 'gtdb_metadata'
    
    # Specify columns which will have additional features.
    genome_id = mapped_column(String, primary_key=True)
    release = mapped_column(Integer)
    
    gtdb_amino_acid_seqs = relationship('AminoAcidSeqs')
    gtdb_annotations_kegg = relationship('AnnotationsKegg')
    
    gtdb_metadata_history = relationship('MetadataHistory')


class AminoAcidSeqs(Reflected, Base):

    __tablename__ = 'gtdb_amino_acid_seqs'

    gene_id = mapped_column(String, ForeignKey('gtdb_annotations_kegg.gene_id'), primary_key=True)
    genome_id = mapped_column(String, ForeignKey('gtdb_metadata.genome_id'))
    
    gtdb_metadata = relationship('Metadata', viewonly=True)
    gtdb_annotations_kegg = relationship('AnnotationsKegg', back_populates='gtdb_amino_acid_seqs')


class AnnotationsKegg(Reflected, Base):
    __tablename__ = 'gtdb_annotations_kegg'

    annotation_id = mapped_column(String, primary_key=True)
    genome_id = mapped_column(String, ForeignKey('gtdb_metadata.genome_id'))

    # Because the relationship from amino acid sequences to annotations should be one-to-one, should specify uselist=False. 
    gtdb_amino_acid_seqs = relationship('AminoAcidSeqs', back_populates='gtdb_annotations_kegg', uselist=False)
    gtdb_metadata = relationship('Metadata', viewonly=True)


class AnnotationsPfam(Reflected, Base):
    __tablename__ = 'gtdb_annotations_pfam'
    
    annotation_id = mapped_column(String, primary_key=True)
    genome_id = mapped_column(String, ForeignKey('gtdb_metadata.genome_id'))

    gtdb_amino_acid_seqs = relationship('AminoAcidSeqs', back_populates='gtdb_annotations_pfam', uselist=False)
    gtdb_metadata = relationship('Metadata', viewonly=True)
  


class Database():
    '''The class which mediates the interactions between the database and Flask app.'''

    def __init__(self, engine):
        
        # First need to reflect the current database into Table objects.
        Reflected.prepare(engine)
        self.tables = [Metadata, AminoAcidSeqs, AnnotationsKegg, AnnotationsPfam, 
            MetadataHistory, AminoAcidSeqsHistory, AnnotationsKeggHistory, AnnotationsPfamHistory]
    
    def get_field_to_table_map(self, fields:Set[str], table:sqlalchemy.Table=None) -> Dict[str, sqlalchemy.Table]:
        '''Returns a dictionary mapping the fields in the input to the SQLAlchemy table in which
        they are found. Minimizes the number of tables required to cover the fields.
        
        :param fields: The fields which must be covered in the set of tables returned by this function. 
        :param table: The main query table, i.e. the one which corresponds to the requested resource. 
        :return: A dictionary mapping each input field to a table which contains it. 
        '''
        # Initialize the field_to_table_map with the fields in the main table. 
        # This will hopefully avoid any unnecessary joins. 
        field_to_table_map = {f:table for f in fields.intersection(self.get_fields(table))}
        fields = fields - self.get_fields(table) # Remove the fields which have already been added. 

        # Sort the tables according to the number of columns they "cover." 
        all_tables = sorted(self.tables, key=lambda t: len(t.__table__.c))
        all_tables.remove(table) # Exclude the main query table. 
        for t in all_tables:
            if len(fields) == 0:
                break
            field_to_table_map.update({f:t for f in fields.intersection(self.get_fields(t))})
            fields = fields - self.get_fields(t)

        assert len(fields) == 0, f'database.get_field_to_table_map: The fields {fields} were not mapped to a table.'
        return field_to_table_map

    def get_fields(self, table:sqlalchemy.Table) -> Set[str]:
        '''Get all the fields stored in the table.
        
        :param table: A SQLAlchemy table for which to grab the field names. 
        :return: A set of strings representing the columns of the table. 
        '''
        return set([c.name for c in table.__table__.c])

    def get_table(self, name:str) -> sqlalchemy.Table:
        '''Get the table corresponding to the given name.
        
        :param name: The name of the table to grab.
        :return: The SQLAlchemy table object with the specified name. 
        :raise: ValueError if the input table name does not match any in the database. 
        '''
        # Probably should treat this like a dictionary, eventually. 
        for table in self.tables:
            if table.__tablename__ == name:
                return table

        raise ValueError('database.FindABugDatabase.get_table: Table {name} is not present in the database.')
   
