'''
Class for managing client-side queries to the Find-A-Bug database (i.e. queries
received at the remote host from a url or the Find-A-Bug API.  
'''
from sqlalchemy import or_, func, desc, select, text
from warnings import warn
from exceptions import FindABugQueryError
# import ast

# TODO: Query API is out-of-date. Update to latest version eventually. 

def generate_filter(table, field, operator, value):
    '''
    Generates a filter which can be used in the Query.filter method.

    args:
        : table (sqlalchemy.Table): The table containing the field to which
            filter is applied. 
        : field (str): The field to which the filter is applied. 
        : operator (str): One of 'eq', 'lt', 'gt', 'le', 'ge'. Specifies the
            operator in the condition set by the filter. 
        : value: The value against which data in the field column is
    '''
    # Extract the relevant column from the provided table. 
    col = getattr(table, field)

    if operator == 'eq':
        return col == value
    if operator == 'ge':
        return col >= value
    if operator == 'le':
        return col <= value
    if operator == 'lt':
        return col < value
    if operator == 'gt':
        return col > value

    # Already checked to make sure the operator strings were valid in the
    # parse_options function. 


def parse_options(url_options):
    '''
    Parse the string of options. String should be of the form
    field=value;operator+field=value;operator+....

    args:
        : url_options (string)
    returns:
        : options (dict)
    '''
    options = {}
    # In the event no filter options are specified. 
    if url_options is None:
        return options

    for option in url_options.split('+'):
        
        field = option.split('=')[0]
        filter_ = option.split('=')[1]
        operator, value = filter_.split(';')
        
        # Infer the type of the value. Should only be floats, ints, and strings.
        try: # See if it can be converted to a float. I guess just treat ints as floats. 
            value = float(value)
        except:
            pass
        
        if type(value) not in [float, int, str]:
            msg = f'The type of {value} does not appear in the Find-A-Bug database.'
            raise FindABugQueryError(msg)

        # Check the operator to make sure it's valid. 
        if operator not in ['eq', 'lt', 'gt', 'le', 'ge']:
            msg = f'{operator} is not a valid operator. Must be one of eq, ge, lt, gt.'
            raise FindABugQueryError(msg)

        if field not in options:
            options[field] = [(operator, value)]
        else:
            options[field].append((operator, value))

    return options


class FindABugQuery():
    '''
    '''
    
    def __init__(self, url_query, url_options, type_='default'):
        '''
        Initialize a FindABugQuery object.

        args:
            : url_query (str):
            : url_options (str): 
        kwargs:
        '''
        
        self.options = parse_options(url_options)
        self.cols = url_query.split('+')
        
        self.stmt = None
        self.type_ = type_
    
    def __repr__(self):
        return f'<FindABugQuery, cols={self.cols}, type={self.type_}>'
    
    def add_filters(self, stmt):
        '''
        Add the filters specified in the self.options attribute to a SQLAlchemy stmt.

        args:
            : stmt (sqlalchemy.Select): A SQLALchemy Select construct. 
        '''
        for col, filter_ in self.options.items():
            table = self.col_to_table[col] 

            # If multiple filters are specified, for a single field, join them
            # using an or statement. 
            if type(filter_) == list:
                or_stmt = []
                # Each filter is a two-tuple, with the operator as the first element
                # and the second is the threshold/thing to match/etc.
                for operator, value in filter_:
                    or_stmt.append(generate_filter(table, col, operator, value))
                stmt = stmt.filter(or_(*or_stmt))
            
            # Execute this block if only a single fiter is specified. 
            elif type(filter_) == tuple:
                operator, value = filter_
                stmt = stmt.filter(generate_filter(table, col, operator, value))
            
            else:
                msg = f'There is an error in the option for the {field} field.'
                raise FindABugQueryError(msg)

            return stmt

    def add_joins(self, stmt):
        '''
        Add all necessary JOINs to a SQLAlchemy stmt.

        args:
            : stmt (sqlalchemy.Select): A SQLALchemy Select construct. 
        '''
        for t1 in [self.col_to_table[c] for c in self.options.keys()]:
            for t2 in [self.col_to_table[c] for c in self.cols]:
                # Extract the relevant ORM relationship on which to join. 
                relationship = getattr(t1, t2.__tablename__, False)
                
                if relationship:
                    stmt = stmt.join(relationship)
        
        return stmt 

    def default(self):
        '''
        Construct a regular query, i.e. a query which returns the requested fields with
        the specified filters applied. 
        '''

        stmt = select(*[getattr(self.col_to_table[c], c) for c in self.cols])
        
        stmt = self.add_joins(stmt)
        stmt = self.add_filters(stmt)
       
        return stmt

    def count(self):
        '''
        Convert the constructed query to a query which returns the count (i.e. the number 
        of results contained in the query.
        '''
       
        # Extract the SQLAlchemy Column from the matching Table.
        col = getattr(self.col_to_table[self.cols[0]], self.cols[0])
        col = func.count(col).label('count')
        
        stmt = select(col)

        stmt = self.add_joins(stmt)
        stmt = self.add_filters(stmt)
 
        return stmt

    def mode(self):
        '''
        Construct a query which returns the most commonly-occurring 
        value in the selected column (the mode). 

        https://stackoverflow.com/questions/28033656/finding-most-
        frequent-values-in-column-of-array-in-sql-alchemyhttps://stackoverflow.com/
        questions/28033656/finding-most-frequent-values-in-column-of-array-in-sql-alchemy
        '''
        # For this to be called, only one column can be specified. 
        if len(self.cols) > 1:
            msg = 'Only one query column may be specified when retrieving mode.'
            raise FindABugQueryError(msg)
        
        # Extract the Column object from the matching table. 
        col = getattr(self.col_to_table[self.cols[0]], self.cols[0])
        
        stmt = select(col, func.count(col).label('frequency'))
        
        stmt = self.add_joins(stmt)
        stmt = self.add_filters(stmt)
 
        # The GROUP BY statement groups rows that have the same values into summary rows.
        stmt = stmt.group_by(col)
        stmt = stmt.order_by(desc(text('frequency')))

        self.cols.append('frequency') # Add a header to the constructed frequency column.

        # Only return the top n most frequently-occurring things. 
        return stmt.limit(5) # No issue if this is None.

    def build(self, session):
        '''
        Using an existing session to construct a SQL query
        using the specifications given when the FindABugQuery object was instantiated. 
        Stores the resulting Select object in the stmt attribute. 

        args:
            : session (sqlalchemy.Session): The SQLAlchemy Session created in
                FindABug object instance. 
        '''
        # Create a map for each field to the table in which it is found.
        # This (and other relevant fields) will be stored in the FindABugQuery.
        self.init_col_to_table_map(session.info['tables'])   

        if self.type_ == 'default':
            self.stmt = self.default()
        elif self.type_ == 'count':
            self.stmt = self.count()
        elif self.type_ == 'mode':
            self.stmt = self.mode()
        else:
            warn('The specified query type is invalid. "default" is being used.')
            self.stmt = self.default()

    def init_col_to_table_map(self, tables):
        '''
        Generate and store a dictionary mapping the fields to the table in which they
        appear. Optimized by using the minimum number of tables possible. 

        args:
            : tables: Contains the objects which represent tables in the SQL
                database (those defined in database/__init__.py). 
        '''
        col_to_table = {}
        
        # Create a list containing both option and main query columns.
        all_cols = self.cols + list(self.options.keys())

        # Map of the tables to the fields they contain. 
        table_to_col = {t:[c for c in all_cols if c in dir(t)] for t in tables} 
        
        # Sort the tables according to the number of columns they "cover"
        ordered_tables = sorted(list(table_to_col.items()), key=lambda x: len(x[1]))
        ordered_tables = [t for t, _ in ordered_tables]

        # Keep track of the fields which have not been covered. 
        unaccounted_for = all_cols[:]
        while len(unaccounted_for) > 0:

            try:
                t = ordered_tables.pop() # Get the table with the most associated fields.
            except IndexError:
                # Happens when there are still unaccounted for fields, but no
                # tables to cover them. 
                msg = f"{', '.join(unaccounted_for)} are not found in the Find-A-Bug database."
                raise FindABugQueryError(msg)
  
            for c in table_to_col[t]: 
                if c in unaccounted_for:
                    col_to_table[c] = t # Associate the field with a table.
                    unaccounted_for.remove(c) 
         
        self.col_to_table = col_to_table


