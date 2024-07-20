'''Class for managing client-side queries to the Find-A-Bug database. Defines the FindABugQuery class, which parses a URL sent to the 
server from a client, and builds a query which can be sent to the SQL database.'''
from sqlalchemy import or_, select 
from sqlalchemy.schema import Column
from sqlalchemy.sql.expression import Select
# from versioned import versioned_session
from typing import Set, List, Dict, NoReturn, Tuple
from utils.tables import Metadata
import sqlalchemy

# Allowed operators... [eq], [gt], [gte], [lt], [lte], [to], [and]

# ko[and]gene_id[eq]x[or]y[or]z[and]e_value[gt]x[and]threshold[eq]a[to]b
# ko    gene_id[eq]x[or]y[or][z]    e_value[gt]x    threshold[eq]a[to]b

# NOTE: Thinking that it is not worth supporting OR statements in the query string, as you could just do multiple queries.
# Might make sense to put the onus of doing this on the user. 




class Query():
    
    def __init__(self, database, table_name:str, page:int=None):

        self.table = database.get_table(table_name)
        self.stmt = select(*self.table.__table__.c) # I don't know why I need to add the columns manually...
        self.page = page
        self.page_size = 500


    def __str__(self):
        '''Return a string representation of the query, which is the statement sent to the SQL database.
        Mostly for debugging purposes.'''
        # This is a potential security risk. See https://feyyazbalci.medium.com/parameter-binding-f0b8df2cf058. 
        return str(self.stmt.compile(compile_kwargs={'literal_binds':True}))

    def submit(self, database):

        # Handling pagination if a page is specified. 
        if self.page is not None:
            # Use orderby to enforce consistent behavior. All tables have a genome ID, so this is probably the simplest way to go about this. 
            self.stmt = self.stmt.order_by(getattr(self.table, 'genome_id'))
            self.stmt = self.stmt.offset(self.page * self.page_size).limit(self.page_size)

        # return database.session.execute(self.stmt.where(Metadata.genome_id == 'GCA_000248235.2'))
        return database.session.execute(self.stmt) # .all()
    

class HistoryQuery(Query):
    pass



class Filter():

    operators = ['[eq]', '[gt]', '[gte]', '[lt]', '[lte]', '[in]']
    symbols = ['[to]', '[or]']
    connector = '[and]'

    @classmethod
    def get_operator(cls, filter_:str):
        for operator in cls.operators:
            if operator in filter_:
                return operator
        return None

    @classmethod
    def parse(cls, filter_string:str):
        
        filters = dict()
        include = []

        for filter_ in filter_string.split(Filter.connector):
            operator = Filter.get_operator(filter_)
            if operator is not None:
                field, value = filter_.split(operator)
                filters[field].append((operator, value))
            else:
                include.append(filter_)

        return filters, include


    def __init__(self, database, table_name:str, filter_string:str):

        self.table_name = table_name 
        self.table = database.get_table(table_name)
        
        self.filters, self.include = Filter.parse(filter_string)

        self.field_to_table_map = dict()
        for rel, _ in self.table.__mapper__.relationships.items():
            rel_table = database.get_table(rel)
            self.field_to_table_map.update({col.name:rel_table for col in rel_table.__table__.c})

        self.tables_to_join = [self.field_to_table_map.get(field) for field in list(self.filters.keys()) + self.include]

    def get_column(self, field:str):
        '''Get the Column object corresponding to the specified field.'''
        table = self.field_to_table_map[field]
        return getattr(table, field)

    def equal_to(self, stmt:Select, col:Column=None, value:str=None):
        if '[or]' in value:
            return stmt.filter(col.in_(value.split('[or]')))
        else:
            return stmt.filter(col == value)

    def less_than(self, stmt:Select, col:Column=None, value:str=None):
        return stmt.filter(col < float(value))
         
    def less_than_or_equal_to(self, stmt:Select, col:Column=None, value:str=None):
        return stmt.filter(col <= float(value))

    def greater_than(self, stmt:Select, col:Column=None, value:str=None):
        return stmt.filter(col > float(value))

    def greater_than_or_equal_to(self, stmt:Select, col:Column=None, value:str=None):
        return stmt.filter(col >= float(value))

    def in_range(self, stmt:Select, col:Column=None, value:str=None):
        low, high = value.split('[to]')
        return stmt.filter(col.between(float(low), float(high)))


    def __call__(self, query:Query):

        stmt = query.stmt
        
        for relationship in self.tables_to_join:
            # TODO: Should probably have a failure condition here if a relationship is not found. 
            if relationship is not None:
                stmt = stmt.join(getattr(self.table, relationship.__table__.name))
            # I don't think we can use joinedload with a many-to-one relationship and get the behavior I want. 
            # stmt = stmt.option(sqlalchemy.orm.joinedload(getattr(table, relationship)))

        # Add all relevant columns to the return statement.
        # for field in self.include + list(self.filters.keys()):
        #     stmt = stmt.add_columns(self.get_column(field))

        for field, (operator, value) in self.filters.items():
            col = self.get_column(field)

            if operator == '[lt]':
                stmt = self.less_than(stmt, col, value)
            elif operator == '[gt]':
                stmt = self.greater_than(stmt, col, value)
            elif operator == '[gte]':
                stmt = self.greater_than_or_equal_to(stmt, col, value)
            elif operator == '[lte]':
                stmt = self.less_than_or_equal_to(stmt, col, value)
            elif operator == '[eq]':
                stmt = self.equal_to(stmt, col, value)
            elif operator == '[in]':
                stmt = self.in_range(stmt, col, value)
        
        query.stmt = stmt # Modify the query that was passed in to the filter. 
        return query # Return the query so that methods can be chained. 





     