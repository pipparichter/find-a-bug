import sqlalchemy
from sqlalchemy import insert
from utils.tables import Proteins, ProteinsHistory, Metadata, MetadataHistory, AnnotationsKegg, AnnotationsKeggHistory, AnnotationsPfamHistory, AnnotationsPfam, Reflected
from typing import List, Dict, NoReturn

class Database():

    # host = '127.0.0.1' # Equivalent to localhost, although not sure why this would work and localhost doesn't.
    host = 'localhost' # Equivalent to localhost, although not sure why this would work and localhost doesn't.
    dialect = 'mariadb'
    driver = 'pymysql'
    user = 'root'
    password = 'Doledi7-Bebyno2'
    name = 'findabug'
    url = f'{dialect}+{driver}://{user}:{password}@{host}/{name}'

    def __init__(self, reflect:bool=True):

        self.engine = sqlalchemy.create_engine(Database.url)
        if reflect:
            Reflected.prepare(self.engine)

        self.session = sqlalchemy.orm.Session(self.engine, autobegin=True)
        self.tables = [Metadata, MetadataHistory, Proteins, ProteinsHistory, AnnotationsKegg, AnnotationsKeggHistory, AnnotationsPfamHistory, AnnotationsPfam]
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
        if self.has_table(table_name): # Only try to drop tables that actually exist.
            table = self.get_table(table_name)
            table.__table__.drop(self.engine)

    def drop_all(self) -> NoReturn:
        '''Delete all tables found in the Database.'''
        # Due to relationships between tables, the AnnotationsHistory tables need to be deleted first, followed by the ProteinsHistory table, 
        # and then the MetadataHistory table.
        existing_tables = self.get_existing_tables()
        table_names = [table_name for table_name in self.table_names[::-1] if table_name in existing_tables]

        for table_name in table_names:
            self.drop(table_name)

    def create(self, table_name:str):

        table = self.get_table(table_name)
        table.__table__.create(bind=self.engine)

    def create_all(self, drop_existing:bool=True):
        # Due to relationships between tables, the MetadataHistory table needs to be created first, followed by the ProteinsHistory
        # table, and then the Annotations tables. They need to be deleted in the opposite order. 
        for table_name in self.table_names:
            self.create(table_name, drop_existing=drop_existing)

    def bulk_upload(self, table_name:str, entries:List[Dict]) -> NoReturn:
        # Sometimes the list of entries is empty, which can cause some errors with SQLAlchemy. 
        if len(entries) > 0:
            table = self.get_table(table_name)
            self.session.execute(insert(table), entries) 
            self.session.commit()

    def move_to_history(self, table_name:str):
        '''Move a current table to history.'''
        assert 'history' not in table_name
        history_table_name = table_name + '_history'

    def reflect(self):
        Reflected.prepare(self.engine)

    def close(self):
        self.session.close()
