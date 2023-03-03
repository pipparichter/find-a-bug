'''
API utilities for accessing information from the SQL database. These utilities
are based on the Kegg REST API. https://biopython.org/docs/1.76/api/Bio.KEGG.REST.html
'''
import sys
import os

curr_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(curr_dir)
sys.path.append('/home/prichter/find-a-bug')

import sqlalchemy
from sqlalchemy import or_
from sqlalchemy import inspect
import sqlalchemy.orm
# import numpy as np
import pandas as pd
from database import database_init
# import os
import pickle

class FindABug():
    '''
    '''

    def __init__(self, engine):
        
        # First need to reflect the current database into Table objects.
        database_init(engine)
        
        # Need to import the tables after they have been initialized in the
        # tables module using the Base class. 
        from database import Metadata_r207, AASeqs_r207, AnnotationsKegg_r207
        self.table_objs = [Metadata_r207, AASeqs_r207, AnnotationsKegg_r207]

        Session = sqlalchemy.orm.sessionmaker(bind=engine)
        self.session = Session()


    def __generate_filter(table, field, operator, value):
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

    # API --------------------------------------------------------------------------
    
    def info(self):
        '''
        Displays information related to the database.

        '''
        df = {'name':[], 'primary_key':[], 'columns':[]}

        for t in self.table_objs:
            df['name'].append(t.__tablename__)
            df['primary_key'].append(inspect(t).primary_key[0].name)
            df['columns'].append([col.name for col in t.__table__.c])
        
        return pd.DataFrame(df)

    def __query_database(self, query_fields, options={}):
        '''
        Finds entries in the specified database which match the given query. 

        args: 
            : query_field (list): The search query, i.e. the fields in which to search.
        kwargs:
            : options (dict): A dictionary where the key is the field and the value
                is some kind of filter to be applied to that field.
        '''
        options_fields = list(options.keys())
        # Create a map for each field to the table in which it is found.
        f2t = self.__get_field_to_table_map(query_fields + options_fields)

        # Start the query with the column being queried. 
        query_tables = [f2t[f] for f in query_fields]
        query_cols = [getattr(t, f) for t, f in zip(query_tables, query_fields)]
        query = self.session.query(*query_cols)
        
        option_tables = [f2t[f] for f in options_fields]
        for t_o in option_tables:
            for t_q in query_tables:
                # Extract the relevant ORM relationship on which to join. 
                relationship = getattr(t_o, t_q.__tablename__, False)
            if relationship:
                query = query.join(relationship)
            
        for field, filter_ in options.items():
            table = f2t[field] 

            # If multiple filters are specified, for a single field, join them
            # using an or statement. 
            if type(filter_) == list:
                or_stmt = []
                for operator, value in filter_:
                    or_stmt.append(FindABug.__generate_filter(table, field,
                        operator, value))
                query = query.filter(or_(*or_stmt))
            
            # Execute this block if only a single fiter is specified. 
            elif type(filter_) == tuple:
                operator, value = filter_
                query = query.filter(FindABug.__generate_filter(table, field,
                    operator, value))
            else:
                msg = f'There is an error in the query option for the {field} field.'
                raise ValueError(msg)
        
        return query

    def __query_to_csv(query):
        '''
        Dumps the information retrieved from a sqlalchemy.Query into a pandas
        DataFrame. 

        args:
            query (sqlalchemy.Query): The query to convert to CSV.
        '''
        csv = []

        # Instantiate a dataframe with the names contained in the query.
        cols = [c['name'] for c in query.column_descriptions]

        csv.append(','.join(cols))
        
        for row in query.all():
            # A row is a tuple of elements, convert to list.
            csv.append(','.join(list(row)))

        return csv

    def query_database(self, query_field, options={}):
        '''
        See FindABug.__query_database.
        '''
        
        query = self.__query_database(query_field, options=options)
        return FindABug.__query_to_csv(query)

        
    # NOTE: I am not convinced that this step was time-limiting, and if the f2t.hist file gets too big, the caching
    # technique I used here might use up more RAM than is worth it. 
    def __get_field_to_table_map(self, fields):
        '''
        Return a dictionary mapping the fields to the table in which they
        appear. Optimized by using the minimum number of tables possible. 

        args:
            : fields (list of str): Contains the names of the fields called in
                a query to the database, both in the main query and the options.
        returns:
            : ft2 (dict of (str, sqlalchemy.Table): A map from the
                fields the table in which the field appears. 
        '''
        # cache = None
        # with open('f2t.hist', 'rb') as history:
        #     cache = pickle.load(history)
        #     
        #     if frozenset(fields) in cache:
        #         print(cache)
        #         return cache[frozenset(fields)]

        # Map of the fields to the tables they are mentioned in. 
        f2t = {}

        # Map of the tables to the fields they contain. 
        t2f = {t:[f for f in fields if f in dir(t)] for t in self.table_objs} 

        items = [(k, v) for k, v in t2f.items()]
        ordered_items = sorted(items, key=lambda x: len(x[1]))
        ordered_keys = [t for t, _ in ordered_items]

        # Keep track of the fields which have not been covered. 
        unaccounted_for = fields[:]
        while len(unaccounted_for) > 0:
            t = ordered_keys.pop() # Get the table with the most associated fields.
            for f in t2f[t]: 
                if f in unaccounted_for:
                    f2t[f] = t # Associate the field with a tail.
                    unaccounted_for.remove(f) 
        
        # # Log the query. 
        # cache[frozenset(fields)] = f2t
        # with open('f2t.hist', 'wb') as history:
        #     pickle.dump(cache, history)
        
        return f2t

