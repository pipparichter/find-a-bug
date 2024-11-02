'''Class for managing client-side queries to the Find-A-Bug database. Defines the FindABugQuery class, which parses a URL sent to the 
server from a client, and builds a query which can be sent to the SQL database.'''
from sqlalchemy import or_, select 
from sqlalchemy.schema import Column
from sqlalchemy.sql.expression import Select
# from versioned import versioned_session
from typing import Set, List, Dict, NoReturn, Tuple
import sqlalchemy
from sqlalchemy.inspection import inspect
from sqlalchemy import func

# Allowed operators... [eq], [gt], [gte], [lt], [lte], [to], [and]

# ko[and]gene_id[eq]x[or]y[or]z[and]e_value[gt]x[and]threshold[eq]a[to]b
# ko    gene_id[eq]x[or]y[or][z]    e_value[gt]x    threshold[eq]a[to]b

# NOTE: Thinking that it is not worth supporting OR statements in the query string, as you could just do multiple queries.
# Might make sense to put the onus of doing this on the user. 


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
                filters[field] = (operator, value)
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
        # Make sure to add the columns in the table itself, to which filters can also be applied. 
        self.field_to_table_map.update({col.name:self.table for col in self.table.__table__.c})
        raise Exception(list(self.table.__mapper__.relationships.items()))

        tables_to_join = [self.field_to_table_map.get(field) for field in list(self.filters.keys()) + self.include]
        tables_to_join = [table for table in tables_to_join if table is not None] # Should I be worried about this?

        # Make sure the table itself is not included in this list. 
        self.tables_to_join = set([table for table in tables_to_join if table.__table__.name != self.table_name])

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


    def __call__(self, stmt):
        
        for relationship in self.tables_to_join:
            # TODO: Should probably have a failure condition here if a relationship is not found. 
            if relationship is not None:
                # TODO: Figure out a better way to handle this...
                table_name = relationship.__table__.name 
                stmt = stmt.join(getattr(self.table, table_name))
            # I don't think we can use joinedload with a many-to-one relationship and get the behavior I want. 
            # stmt = stmt.option(sqlalchemy.orm.joinedload(getattr(table, relationship)))

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
        
        # Add all relevant columns to the return statement.
        selected_columns = [col.name for col in stmt.selected_columns]
        for field in self.include + list(self.filters.keys()):
            col = self.get_column(field)
            if col.name not in selected_columns:
                stmt = stmt.add_columns(col)

        return stmt 



class Query():
    
    def __init__(self, database, table_name:str, page:int=0, page_size:int=None, filter_string:str=None):

        self.table = database.get_table(table_name)
        self.table_primary_key = inspect(self.table).primary_key[0].name
        self.page = page
        self.page_size = page_size
        self.filter_ = Filter(database, table_name, filter_string) if (filter_string is not None) else None

    def __str__(self):
        '''Return a string representation of the query, which is the statement sent to the SQL database.
        Mostly for debugging purposes.'''
        # This is a potential security risk. See https://feyyazbalci.medium.com/parameter-binding-f0b8df2cf058. 
        return str(self.stmt.compile(compile_kwargs={'literal_binds':True}))

    def get(self, database, debug:bool=False, filter:Filter=None):
        # Use orderby to enforce consistent behavior. All tables have a genome ID, so this is probably the simplest way to go about this. 
        self.stmt = select(*self.table.__table__.c) # I don't know why I need to add the columns manually...
        self.stmt = self.stmt.order_by(getattr(self.get_outer_table(database), 'genome_id'))
        if self.filter_ is not None:
            self.stmt = self.filter_(self.stmt)
        if self.page_size is not None:
            self.stmt = self.stmt.offset(self.page * self.page_size).limit(self.page_size)

        # return database.session.execute(self.stmt.where(Metadata.genome_id == 'GCA_000248235.2'))
        if debug:
            return str(self)

        return database.session.execute(self.stmt) # .all()

    def count(self, database, debug:bool=False, filter_:Filter=None):
        # Modified from https://gist.github.com/hest/8798884
        # NOTE: Why are subqueries so bad?
        # self.stmt = select(func.count(self.table.__table__)) # I don't know why I need to add the columns manually...)
        self.stmt = select(func.count(getattr(self.table, self.table_primary_key))) # I don't know why I need to add the columns manually...)
        self.stmt = self.stmt.order_by(None)
        if self.filter_ is not None:
            self.stmt = self.filter_(self.stmt)

        if debug:
            return str(self)

        return database.session.execute(self.stmt).scalar()


    def get_outer_table(self, database):
        '''The database engine picks a table for the "outer" part of the query, i.e. the table on the left side of the join (this table is not always the first one
        added to the select statement). If the ORDER BY does not use this outer table, then it creates a temporary table with the outer table's values sorted according to the 
        inner table's values (I think), which is very slow and uses a lot of memory. this function figures out what the outer table is, and ensures that the ORDER BY is 
        called on that table.'''

        sql = self.__str__()
        result = database.explain(self)  
        outer_table_name = result['table'].values[0] # Get the first row from the result of EXPLAIN.
        return database.get_table(outer_table_name)


    

class HistoryQuery(Query):
    pass







     