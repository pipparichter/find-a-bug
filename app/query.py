'''Class for managing client-side queries to the Find-A-Bug database. Defines the FindABugQuery class, which parses a URL sent to the 
server from a client, and builds a query which can be sent to the SQL database.'''
from warnings import warn
from sqlalchemy import or_, func, desc, select, text, column, Table
from sqlalchemy.schema import Column
# from versioned import versioned_session
from urllib.parse import parse_qsl, urlparse
from typing import Set, List, Dict, NoReturn, Tuple
from app.tables import Database
import sqlalchemy

# URL format ------------------------------------------------------------------------------
# https://microbes.gps.caltech.edu/get/annotations_kegg?{x1}={y1}&{x2}={y2}...#page
# https://microbes.gps.caltech.edu/get/annotations_pfam?{x1}={y1}&{x2}={y2}...#page
# https://microbes.gps.caltech.edu/get/metadata?{x1}={y1}&{x2}={y2}...#page
# https://microbes.gps.caltech.edu/get/proteins?{x1}={y1}&{x2}={y2}...#page
# -----------------------------------------------------------------------------------------

# Allowed operators... [eq], [gt], [gte], [lt], [lte], [to], [and]

# ko[and]gene_id[eq]x[or]y[or]z[and]e_value[gt]x[and]threshold[eq]a[to]b
# ko    gene_id[eq]x[or]y[or][z]    e_value[gt]x    threshold[eq]a[to]b

# NOTE: Thinking that it is not worth supporting OR statements in the query string, as you could just do multiple queries.
# Might make sense to put the onus of doing this on the user. 

OPERATORS = ['=', '<', '>']


class Filter():

    def __int__(self, string):
        pass 


class Query():
    
    def __init__(self, url, database:Database):
        '''Initialize a  object.'''

        url = urlparse(url) # Parse the URL string.
        table_name = url.path.replace('/get/', '') # One of annotations, sequences, or metadata.
        table = database.get_table(table_name)
        # Collect all fields used in the query. 
        fields = set(fields).union({field for field, _ in query_list})
        field_to_table_map = db.get_field_to_table_map(fields, table=table)
 
        self.stmt = select(table)

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


class HistoryQuery():
    pass







