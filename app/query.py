'''Class for managing client-side queries to the Find-A-Bug database. Defines the FindABugQuery class, which parses a URL sent to the 
server from a client, and builds a query which can be sent to the SQL database.'''
from warnings import warn
from sqlalchemy import or_, func, desc, select, text, column
from urllib.parse import parse_qsl, urlparse
import typing 
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

def get_filter(col:sqlalchemy.schema.Column, operator:str, value):
    '''Generates a filter which can be added to a SQLAlchemy Select object. 

    args:
        - table: The table containing the field to which filter is applied. 
        - field: The field to which the filter is applied. 
        - operator: One of 'eq', 'lt', 'gt', 'le', 'ge'. Specifies the operator in the condition set by the filter. 
        - value: The value against which data in the field column is compared.
    '''
    # Extract the relevant column from the provided table. 
    if operator == '=':
        return col == value
    if operator == '<':
        return col < value
    if operator == '>':
        return col > value


class FindABugQuery():
    
    def __init__(self, url, engine):
        '''Initialize a FindABugQuery object.'''

         # Create a new session, storing the table names in the info field. 
        Session = sqlalchemy.orm.sessionmaker(bind=engine)
        self.session = Session()
        
        url = urlparse(url) # Parse the URL string.

        # Use this instead of parse_qs to support multiple of the same keys. 
        self.qsl = parse_qsl(url.query, separator='&') # Returns a list of key, value pairs. 
        
        # Reflect the Database. 
        self.db = FindABugDatabase(engine)
        # Get the table corresponding to the specified URL resource.
        self.table = self.db.get_table(url.path[1:]) # Make sure to remove the leading forward slash.
        
        # Collect all fields used in the query, including those affiliated with the main table. 
        self.fields = self.db.get_fields(self.table).union({field for field, _ in self.qsl})
        self.query_tables = self.db.get_query_tables(self.fields)

        # Make sure to include all columns being referenced in the SELECT statement. 
        stmt = select([getattr(t, f) for f, t in self.query_tables.items()])

        stmt = self.add_joins(stmt)
        stmt = self.add_filters(stmt)

        page_size = 500
        page = None if len(url.fragment) == 0 else int(url.fragment)
        if page is not None:
            self.stmt = stmt.offset(self.page_size * self.page).limit(self.page_size)
        else: # If no page specified, grab all results. 
            self.stmt = stmt

    def execute(self):
        '''Run the query, grabbing the requested information from the database.'''
        # Grab the queries corresponding to the requested page. 
        return self.session.execute(self.stmt).all()
    
    def add_filters(self, stmt:sqlalchemy.sql.expression.Select) -> sqlalchemy.sql.expression.Select:
        '''Add the query specifications in the self.qsl list to the query statement.'''
        
        for field, val in self.qsl:
            table = self.query_tables[field]
            if val[0] in OPERATORS:
                stmt = stmt.filter(get_filter(getattr(table, field), operator, val))
            else:
                stmt = stmt.filter(get_filter(getattr(table, field), '=', val)) 
        return stmt

    def add_joins(self, stmt:sqlalchemy.sql.expression.Select) -> sqlalchemy.sql.expression.Select:
        '''Add all necessary JOINs to the Select statement..'''

        for field in self.query_tables.keys():
            # Extract the relevant ORM relationship on which to join. 
            relationship = getattr(self.table, query_tables[field].__tablename__, False)
            if relationship:
                stmt = stmt.join(relationship)
        return stmt






