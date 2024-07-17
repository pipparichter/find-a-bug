import sqlalchemy
from sqlalchemy import insert
from utils.tables import Proteins, ProteinsHistory, Metadata, MetadataHistory, AnnotationsKegg, AnnotationsKeggHistory, AnnotationsPfamHistory, AnnotationsPfam, Reflected
from typing import List, Dict, NoReturn

class Database():

    host = '127.0.0.1' # Equivalent to localhost, although not sure why this would work and localhost doesn't.
    dialect = 'mariadb'
    driver = 'pymysql'
    user = 'root'
    password = 'Doledi7-Bebyno2'
    name = 'findabug'
    url = f'{dialect}+{driver}://{user}:{password}@{host}/{name}'

    def __init__(self):

        self.engine = sqlalchemy.create_engine(Database.url)
        Reflected.prepare(self.engine)

        self.session = sqlalchemy.orm.Session(self.engine, autobegin=True)
        self.tables = [Proteins, ProteinsHistory, Metadata, MetadataHistory, AnnotationsKegg, AnnotationsKeggHistory, AnnotationsPfamHistory, AnnotationsPfam]
        self.table_names = [table.__tablename__ for table in self.tables]

    def has_table(self, table_name:str) -> bool:
        '''Checks for the existence of a table in the database.'''
        return sqlalchemy.inspect(self.engine).has_table(table_name)

    def get_table(self, table_name:str):
        idx = self.table_names.index(table_name)
        return self.tables[idx]

    def get_existing_tables(self):
        return self.engine.table_names()

    def drop(self, table_name:str) -> NoReturn:
        '''Deletes a SQL table from the SQL database'''
        if self.has_table(table_name):
            table = self.get_table(table_name)
            table.__table__.drop(self.engine)

    def drop_all(self) -> NoReturn:
        '''Delete all tables found in the Database.'''
        for table_name in self.get_existing_tables():
            self.drop(table_name)

    def create(self, table_name:str, drop_existing:bool=True):

        # If specified, drop the table if it already exists. 
        if self.has_table(table_name) and drop_existing:
            self.drop(table_name)

        table = self.get_table(table_name)
        table.__table__.create(bind=self.engine)

    def create_all(self, drop_existing:bool=True):

        for table_name in self.table_names:
            self.create(table_name, drop_existing=drop_existing)

    def bulk_upload(self, table_name:str, entries:List[Dict]) -> NoReturn:

        table = self.get_table(table_name)
        self.session.execute(insert(table), entries) 

    def move_to_history(self, table_name:str):
        '''Move a current table to history.'''
        assert 'history' not in table_name
        history_table_name = table_name + '_history'

    def close(self):
        self.session.close()
