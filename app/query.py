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
    
    def __init__(self, url, engine, page=None):
        '''Initialize a FindABugQuery object.'''

         # Create a new session, storing the table names in the info field. 
        Session = sqlalchemy.orm.sessionmaker(bind=engine)
        self.session = Session()

        # Reflect the Database. 
        db = FindABugDatabase(engine)
        
        url = urlparse(url) # Parse the URL string.
        resource = url.path.replace('/api/', '') # One of annotations, sequences, or metadata. 
        # Define the minimum fields to return when each resource is queried. Also define the primary field, which is what
        # is used to order the response data. These correspond to the primary keys of the tables. 
        if resource == 'annotations':
            table = db.get_table('gtdb_r207_annotations_kegg')
            fields = ['gene_id', 'ko', 'genome_id', 'annotation_id'] # Minimum fields to return for this table. 
            primary_field = 'annotation_id'
        elif resource == 'sequences':
            table = db.get_table('gtdb_r207_amino_acid_seqs')
            fields = ['gene_id', 'seq', 'genome_id', 'nt_start', 'nt_stop', 'reverse'] # Minimum fields to return for this table. 
            primary_field = 'gene_id'
        elif resource == 'metadata':
            table = db.get_table('gtdb_r207_metadata')
            fields = ['genome_id'] # Minimum fields to return for this table. 
            primary_field = 'genome_id'
        else:
            raise Exception(f'query.FindABugQuery: Invalid resource {resource}.')
        
        query_list = parse_qsl(url.query, separator='&') # Returns a list of key, value pairs. This may be empty. 
        # Collect all fields used in the query, including those affiliated with the main table. 
        fields = set(fields).union({field for field, _ in query_list})
        field_to_table_map = db.get_field_to_table_map(fields, table=table)

        # Make sure to include all columns being referenced in the SELECT statement. 
        self.stmt = select(*[getattr(t, f) for f, t in field_to_table_map.items()])

        self.add_joins(table=table, tables=[t for t in field_to_table_map.values()])
        self.add_filters(query_list=query_list, field_to_table_map=field_to_table_map )

        # Handling pagination if a page is specified. 
        if page is not None:
            page_size = 500
            # Use orderby to enforce consistent behavior. 
            self.stmt = self.stmt.order_by(getattr(table, primary_field))
            self.stmt = self.stmt.offset(page * page_size).limit(page_size)

    def execute(self):
        '''Run the query, grabbing the requested information from the database.'''
        output = self.session.execute(self.stmt).all()
        self.session.close() # Trying to avoid pool limit issues.
        return output

    def add_filters(self, query_list:List[Tuple[str, str]]=None, field_to_table_map:Dict[str, Table]=None) -> NoReturn:
        '''Add the specifications in the query list to the statement attribute.
        
        :param query_list: A list of tuples mapping a field name to a string of the form {operator}{value}. 
        :param field_to_table_map: A dictionary mapping a field name to a SQLAlchemy Table object.         
        '''
        for field, val in query_list:
            # Asterisk means that no filter is applied, just include the values associated with the field.
            if val == '*':
                continue
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

    def __str__(self):
        '''Return a string representation of the query, which is the statement sent to the SQL database.
        Mostly for debugging purposes.'''
        # This is a potential security risk. See https://feyyazbalci.medium.com/parameter-binding-f0b8df2cf058. 
        return str(self.stmt.compile(compile_kwargs={'literal_binds':True}))







