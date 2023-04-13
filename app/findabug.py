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
from database import database_init


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
        
        # Create a new session, storing the table names in the info field. 
        Session = sqlalchemy.orm.sessionmaker(bind=engine)
        self.session = Session(info={'tables':self.tables})

    def info(self):
        '''
        Displays information related to the database.

        '''
        response = []

        for t in self.tables:
            response.append(f'NAME: {t.__tablename__}')
            response.append(f'PRIMARY KEY: {inspect(t).primary_key[0].name}')
            cols = ' '.join([col.name for col in t.__table__.c])
            response.append(f'COLUMNS: {cols}')
            response.append('\n')
        
        return response
    
    def query(self, fabq):
        '''
        Finds entries in the specified database which match the given query. 

        args: 
            : fabq (query.FindABugQuery): Defined in the query.py file.
        returns:
            : query (sqlalchemy.Query): A SQLAlchemy Query object.
            : csv (list of str): A list of strings, where each string is a row
                in CSV format. 
        '''
        # TODO: Maybe change how I do this. Perhaps store the query as an attribute in 
        # the FindABugQuery object instead. 

        # Constructs the SQLAlchemy Select object, which can then be executed. 
        fabq.build(self.session)
        # Execute the SQL query and obtain a Result object. 
        result = self.session.execute(fabq.stmt)
        
        # Convert the query result to a "CSV" (a list of strings)
        csv = []
        csv.append(','.join(fabq.cols))

        for row in self.session.execute(fabq.stmt):
            # A row is a tuple of elements, convert to list.
            # Also need to make sure the elements are strings. 
            row = [str(x) for x in list(row)]
            csv.append(','.join(row))
        
        return csv
