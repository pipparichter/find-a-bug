'''Class for managing client-side queries to the Find-A-Bug database. Defines the FindABugQuery class, which parses a URL sent to the 
server from a client, and builds a query which can be sent to the SQL database.'''
from warnings import warn
from sqlalchemy import or_, func, desc, select, text, column, Table
from sqlalchemy.schema import Column
from urllib.parse import parse_qsl, urlparse
from typing import Set, List, Dict, NoReturn, Tuple
from app.database import FindABugDatabase
import sqlalchemy

# URL format ------------------------------------------------------------------------------
# https://microbes.gps.caltech.edu/annotations?{x1}={y1}&{x2}={y2}...#page
# https://microbes.gps.caltech.edu/metadata?{x1}={y1}&{x2}={y2}...#page
# https://microbes.gps.caltech.edu/sequences?{x1}={y1}&{x2}={y2}...#page
# -----------------------------------------------------------------------------------------
# Will eventually probably add stuff like "gtdb/r207/v1" to the front.


# NOTE: Thinking that it is not worth supporting OR statements in the query string, as you could just do multiple queries.
# Might make sense to put the onus of doing this on the user. 

OPERATORS = ['=', '<', '>']

def get_filter(col:Column, operator:str, value:str):
    '''Generates a filter which can be added to a SQLAlchemy Select object. 

    :param col: The SQLAlchemy column object to which the filter will be applied. 
    :param operator: A string representing the comparison operator. One of < > or =. 
    :param value: A string representing the value on the operator right-hand side. 
    :raise: A ValueError if a non-numerical value is provided with a < or > operator. 
    :raise: A ValueError if the operator is not one of < > or =. 
    '''
    # Extract the relevant column from the provided table. 
    if operator == '=':
        return col == value
    # If a < or > is used, the value must be numerical. 
    elif operator == '<':
        return col < float(value)
    elif operator == '>':
        return col > float(value)
    else:
        raise ValueError(f'app.query.get_filter: {operator} is an invalid operator.')


class FindABugQuery():
    
    def __init__(self, url, engine):
        '''Initialize a FindABugQuery object.'''

         # Create a new session, storing the table names in the info field. 
        Session = sqlalchemy.orm.sessionmaker(bind=engine)
        self.session = Session()

        # Reflect the Database. 
        db = FindABugDatabase(engine)
        
        url = urlparse(url) # Parse the URL string.
        resource = url.path.replace('/', '') # One of annotations, sequences, or metadata. 
        if resource == 'annotations':
            table = db.get_table('gtdb_r207_annotations_kegg')
            fields = ['gene_id', 'ko', 'genome_id'] # Minimum fields to return for this table. 
        elif resource == 'sequences':
            table = db.get_table('gtdb_r207_amino_acid_seqs')
            fields = ['gene_id', 'seq', 'genome_id'] # Minimum fields to return for this table. 
        elif resource == 'metadata':
            table = db.get_table('gtdb_r207_metadata')
            fields = ['genome_id'] # Minimum fields to return for this table. 
        
        query_list = parse_qsl(url.query, separator='&') # Returns a list of key, value pairs. This may be empty. 
        # Collect all fields used in the query, including those affiliated with the main table. 
        fields = set(fields).union({field for field, _ in query_list})
        field_to_table_map = db.get_field_to_table_map(fields)

        # Make sure to include all columns being referenced in the SELECT statement. 
        self.stmt = select(*[getattr(t, f) for f, t in field_to_table_map.items()])

        self.add_joins(table=table, tables=[t for t in field_to_table_map.values()])
        self.add_filters(query_list=query_list, field_to_table_map=field_to_table_map )

        page = None if len(url.fragment) == 0 else int(url.fragment)
        if page is not None: # Default page size to 500. 
            self.stmt = self.stmt.offset(page_size * self.page)
        self.stmt = self.stmt.limit(500)

    def execute(self):
        '''Run the query, grabbing the requested information from the database.'''
        return self.session.execute(self.stmt).all()
    
    def add_filters(self, query_list:List[Tuple[str, str]]=None, field_to_table_map:Dict[str, Table]=None) -> NoReturn:
        '''Add the specifications in the query list to the statement attribute.
        
        :param query_list: A list of tuples mapping a field name to a string of the form {operator}{value}. 
        :param field_to_table_map: A dictionary mapping a field name to a SQLAlchemy Table object.         
        '''
        for field, val in query_list:
            table = field_to_table_map[field]
            col = getattr(table, field) # Extract the column from the Table. 
            op, val = ('=', val) if val[0] not in OPERATORS else (val[0], val[1:]) # Default to the equals operator. 
            self.stmt = self.stmt.filter(get_filter(getattr(table, field), op, val))

    def add_joins(self, table:Table=None, tables:List[Table]=None) -> NoReturn:
        '''Add all necessary JOINs to the statement attribute. The minimum fields to include for each table ensures
        that the necessary fields for a JOIN are included.
        
        :param table: The primary table to join, corresponding to the resource specified at instantiation. 
        :param tables: The SQLAlchemy tables which need to be joined to the primary table.
        :return: The SQLAlchemy statement modified to include JOINs. 
        '''
        for t in tables: # Iterate over the field names. 
            # Extract the relevant ORM relationship on which to join. 
            relationship = getattr(table, t.__tablename__, False)
            if relationship:
                self.stmt = self.stmt.join(relationship)







