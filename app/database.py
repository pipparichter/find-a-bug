'''Setting up ORM for the Find-A-Bug database. Reflects SQL tables which already exist, as created by scripts in the data subdirectory.'''
import os
from sqlalchemy import inspect
import sqlalchemy
import sqlalchemy.orm
from sqlalchemy import String, ForeignKey # , ForeignKeyConstraint
from sqlalchemy.orm import DeclarativeBase, relationship, mapped_column
from sqlalchemy.ext.declarative import DeferredReflection
from typing import List, Dict, Set


class Base(DeclarativeBase):
    pass


class Reflected(DeferredReflection):
    '''To accommodate the use case of declaring mapped classes where reflection of table metadata can occur afterwards. 
    It alters the declarative mapping process to be delayed, and will integrate the results with the declarative table
    mapping process.'''
    __abstract__ = True # NOTE: What does this mean?




class Metadata_r207(Reflected, Base):

    release = 207

    __tablename__ = 'gtdb_r207_metadata'
    
    # Specify columns which will have additional features.
    genome_id = mapped_column(String, primary_key=True)
    
    gtdb_r207_amino_acid_seqs = relationship('AASeqs_r207')
    gtdb_r207_annotations_kegg = relationship('AnnotationsKegg_r207')


class AASeqs_r207(Reflected, Base):
    
    release = 207

    __tablename__ = 'gtdb_r207_amino_acid_seqs'

    gene_id = mapped_column(String, ForeignKey('gtdb_r207_annotations_kegg.gene_id'), primary_key=True)
    genome_id = mapped_column(String, ForeignKey('gtdb_r207_metadata.genome_id'))
    
    gtdb_r207_metadata = relationship('Metadata_r207', viewonly=True)
    gtdb_r207_annotations_kegg = relationship('AnnotationsKegg_r207',
            back_populates='gtdb_r207_amino_acid_seqs')
     

class AnnotationsKegg_r207(Reflected, Base):

    release = 207
    annotation_type = 'kegg'

    __tablename__ = 'gtdb_r207_annotations_kegg'
    
    # gene_name = mapped_column(String, primary_key=True)
    annotation_id = mapped_column(String, primary_key=True)
    genome_id = mapped_column(String, ForeignKey('gtdb_r207_metadata.genome_id'))

    # Because the relationship from amino acid sequences to annotations
    # should be one-to-one, should specify uselist=False. Note that here we
    # are considering the annotations table to be the "parent table" to the
    # amino acid sequence table. 
    gtdb_r207_amino_acid_seqs = relationship('AASeqs_r207',
            back_populates='gtdb_r207_annotations_kegg', uselist=False)
    gtdb_r207_metadata = relationship('Metadata_r207', viewonly=True)
    

class FindABugDatabase():
    '''The class which mediates the interactions between the database and Flask app.'''

    def __init__(self, engine):
        
        # First need to reflect the current database into Table objects.
        Reflected.prepare(engine)
        self.tables = [Metadata_r207, AASeqs_r207, AnnotationsKegg_r207]
    
    def get_query_tables(self, fields:Set[str]) -> Dict[str, sqlalchemy.Table]:
        '''Returns a dictionary mapping the fields in the input to the Sqlalchemy table in which
        they are found. Minimizes the number of tables required to cover the fields.'''
        
        # Sort the tables according to the number of columns they "cover"
        all_tables = sorted(self.tables, key=lambda t: len(t.__table__.c))
        query_tables = {}

        for table in all_tables:
            if len(fields) == 0:
                break
            query_tables.update({field:table for field in fields.intersection(self.get_fields(table))})
            fields = fields - self.get_fields(table)
        print(query_tables)
        return query_tables

    def get_fields(self, table:sqlalchemy.Table) -> Set[str]:
        '''Get all the fields stored in the table.'''
        return set([c.name for c in table.__table__.c])

    def get_table(self, name):
        '''Get the table corresponding to the given name.'''
        aliases = {'annotations':'gtdb_r207_annotations_kegg', 'sequences':'gtdb_r207_amino_acid_seqs', 'metadata':'gtdb_r207_metadata'}
        # Probably should treat this like a dictionary, eventually. 
        for table in self.tables:
            if table.__tablename__ == aliases[name]:
                return table

   
