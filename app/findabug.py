'''
API utilities for accessing information from the SQL database. These utilities
are based on the Kegg REST API. https://biopython.org/docs/1.76/api/Bio.KEGG.REST.html
'''
import sys
import os

curr_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(curr_dir)
sys.path.append('/home/prichter/find-a-bug')

from sqlalchemy import inspect
import sqlalchemy
import sqlalchemy.orm
import pandas as pd
from database import database_init


def query_to_csv(query):
    '''
    Dumps the information retrieved from a sqlalchemy.Query into list of
    strings, where each string is a row in a CSV file. 

    args:
        query (sqlalchemy.Query): The query to convert to CSV.
    '''
    csv = []

    # Instantiate a dataframe with the names contained in the query.
    cols = [c['name'] for c in query.column_descriptions]

    csv.append(','.join(cols))
    
    for row in query.all():
        # A row is a tuple of elements, convert to list.
        # Also need to make sure the elements are strings. 
        row = [str(x) for x in list(row)]
        csv.append(','.join(row))

    return csv


class FindABug():
    '''
    The class which handles the the
    '''

    def __init__(self, engine):
        
        # First need to reflect the current database into Table objects.
        database_init(engine)
        
        # Need to import the tables after they have been initialized in the
        # tables module using the Base class. 
        from database import Metadata_r207, AASeqs_r207, AnnotationsKegg_r207
        self.tables = [Metadata_r207, AASeqs_r207, AnnotationsKegg_r207]

        Session = sqlalchemy.orm.sessionmaker(bind=engine)
        self.session = Session()


    def info(self):
        '''
        Displays information related to the database.

        '''
        df = {'name':[], 'primary_key':[], 'columns':[]}

        for t in self.tables:
            df['name'].append(t.__tablename__)
            df['primary_key'].append(inspect(t).primary_key[0].name)
            df['columns'].append([col.name for col in t.__table__.c])
        
        return pd.DataFrame(df)

    def query_database(self, Q):
        '''
        Finds entries in the specified database which match the given query. 

        args: 
            : Q (query.Query)
        '''
        # Create a map for each field to the table in which it is found.
        # This (and other relevant fields) will be stored in the FindABugQuery.
        Q.init_field_to_table_map(self.tables)   
        # Creates a SQLAlchemy Query object. 
        query = Q.build_sql_query(self.session)
        return query, query_to_csv(query)
    

