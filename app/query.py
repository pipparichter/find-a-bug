'''
Class for managing client-side queries to the Find-A-Bug database (i.e. queries
received at the remote host from a url or the Find-A-Bug API.  
'''
from sqlalchemy import or_

from exceptions import FindABugQueryError
# import ast

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
    
    def __init__(self, url_query, url_options):
        
        self.options = parse_options(url_options)
        self.options_fields = list(self.options.keys())
        self.query_fields = url_query.split('+')
        self.all_fields = self.query_fields + self.options_fields

        # These will be populated later. 
        self.query_tables = None
        self.options_tables = None
        self.f2t = None
    
    def __repr__(self):
        return f'<FindABugQuery, fields={self.query_fields}>'
       
    def build(self, session):
        '''
        Using an existing session to construct a SQL query (in the form of a
        SQLAlchemy Query object) using the specifications given when the
        FindABugQuery object was instantiated. 

        args:
            : session (sqlalchemy.Session): The SQLAlchemy Session created in
                FindABug object instance. 
        '''
        # Create a map for each field to the table in which it is found.
        # This (and other relevant fields) will be stored in the FindABugQuery.
        self.init_field_to_table_map(session.info['tables'])   


        query_cols = [getattr(t, f) for t, f in zip(self.query_tables, self.query_fields)]
        query = session.query(*query_cols)
        
        for t_o in self.options_tables:
            for t_q in self.query_tables:
                # Extract the relevant ORM relationship on which to join. 
                relationship = getattr(t_o, t_q.__tablename__, False)
            if relationship:
                query = query.join(relationship)
            
        for field, filter_ in self.options.items():
            table = self.f2t[field] 

            # If multiple filters are specified, for a single field, join them
            # using an or statement. 
            if type(filter_) == list:
                or_stmt = []
                for operator, value in filter_:
                    or_stmt.append(generate_filter(table, field, operator, value))
                query = query.filter(or_(*or_stmt))
            
            # Execute this block if only a single fiter is specified. 
            elif type(filter_) == tuple:
                operator, value = filter_
                query = query.filter(generate_filter(table, field, operator, value))
            
            else:
                msg = f'There is an error in the query option for the {field} field.'
                raise FindABugQuery(msg)

        return query
 
    def init_field_to_table_map(self, tables):
        '''
        Generate and store a dictionary mapping the fields to the table in which they
        appear. Optimized by using the minimum number of tables possible. 

        args:
            : tables: Contains the objects which represent tables in the SQL
                database (those defined in database/__init__.py). 
        '''
        f2t = {}

        # Map of the tables to the fields they contain. 
        t2f = {t:[f for f in self.all_fields if f in dir(t)] for t in tables} 

        items = [(k, v) for k, v in t2f.items()]
        ordered_items = sorted(items, key=lambda x: len(x[1]))
        ordered_keys = [t for t, _ in ordered_items]

        # Keep track of the fields which have not been covered. 
        unaccounted_for = self.all_fields[:]
        while len(unaccounted_for) > 0:

            try:
                t = ordered_keys.pop() # Get the table with the most associated fields.
            except IndexError:
                # Happens when there are still unaccounted for fields, but no
                # tables to cover them. 
                msg = f"{', '.join(unaccounted_for)} are not found in the Find-A-Bug database."
                raise FindABugQueryError(msg)
  
            for f in t2f[t]: 
                if f in unaccounted_for:
                    f2t[f] = t # Associate the field with a table.
                    unaccounted_for.remove(f) 
         
        # Populate some fields to be used in building a SQLAlchemy query later on. 
        self.options_tables = [f2t[f] for f in self.options_fields]
        self.query_tables = [f2t[f] for f in self.query_fields]
        self.f2t = f2t


