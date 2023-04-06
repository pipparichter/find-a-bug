'''
Definitions of the ORM (Object Relational Management) classes, which are mapped to specific tables in the
genome database. This setup includes the establishment of relationships between
tables via the mapped objects. 
'''
from sqlalchemy import String, ForeignKey # , ForeignKeyConstraint
from sqlalchemy.orm import DeclarativeBase, relationship, mapped_column
from sqlalchemy.ext.declarative import DeferredReflection

# Variable which stores whether or not the database information has been
# reflected into the table classes.  
reflected = False

class Base(DeclarativeBase):
    pass

class Reflected(DeferredReflection):
    '''
    To accommodate the use case of declaring mapped classes where 
    reflection of table metadata can occur afterwards. It alters the 
    declarative mapping process to be delayed, and will integrate the
    results with the declarative table mapping process.
    '''
    __abstract__ = True # NOTE: What does this mean?


class Metadata_r207(Reflected, Base):

    release = 207

    __tablename__ = 'gtdb_r207_metadata'
    
    # Specify columns which will have additional features.
    genome_id = mapped_column(String, primary_key=True)
    
    gtdb_r207_amino_acid_seqs = relationship('AASeqs_r207')
    gtdb_r207_annotations_kegg = relationship('AnnotationsKegg_r207')
    

    def __str__(self):
        return f'<Find-A-Bug Table, name={self.__tablename__}>'

    def __repr__(self):
        return self.__str__()


class AASeqs_r207(Reflected, Base):
    
    release = 207

    __tablename__ = 'gtdb_r207_amino_acid_seqs'

    gene_name = mapped_column(String, ForeignKey('gtdb_r207_annotations_kegg.gene_name'), primary_key=True)
    genome_id = mapped_column(String, ForeignKey('gtdb_r207_metadata.genome_id'))
    # Should allow rapid indexing using genus. This is temporary. 
    genus = mapped_column(String, index=True)
    
    gtdb_r207_metadata = relationship('Metadata_r207', viewonly=True)
    gtdb_r207_annotations_kegg = relationship('AnnotationsKegg_r207',
            back_populates='gtdb_r207_amino_acid_seqs')
     
    def __str__(self):
        return f'<Find-A-Bug Table, name={self.__tablename__}>'

    def __repr__(self):
        return self.__str__()
 

class AnnotationsKegg_r207(Reflected, Base):

    release = 207
    annotation_type = 'kegg'

    __tablename__ = 'gtdb_r207_annotations_kegg'
    
    # gene_name = mapped_column(String, primary_key=True)
    unique_id = mapped_column(String, primary_key=True)
    genome_id = mapped_column(String, ForeignKey('gtdb_r207_metadata.genome_id'))

    # Because the relationship from amino acid sequences to annotations
    # should be one-to-one, should specify uselist=False. Note that here we
    # are considering the annotations table to be the "parent table" to the
    # amino acid sequence table. 
    gtdb_r207_amino_acid_seqs = relationship('AASeqs_r207',
            back_populates='gtdb_r207_annotations_kegg', uselist=False)
    gtdb_r207_metadata = relationship('Metadata_r207', viewonly=True)
    
    def __str__(self):
        return f'<Find-A-Bug Table, name={self.__tablename__}>'

    def __repr__(self):
        return self.__str__()



def database_init(engine):
    '''
    Above, we create a mixin class Reflected that will serve as a base for
    classes in our declarative hierarchy that should become mapped when the
    Reflected.prepare method is called. 
    '''
    Reflected.prepare(engine)


def get_all_fields():
    '''
    Get all the fields stored in the database.
    '''
    table_objs = [AnnotationsKegg_r207, Metadata_r207, AASeqs_r207]
    fields = []

    for t in table_objs:
        fields += [str(c) for c in t.__table__.c]

    return fields


