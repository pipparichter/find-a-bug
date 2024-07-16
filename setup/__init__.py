import sqlalchemy
from app import tables
from utils import get_database_url, list_tables, drop_table, table_exists

def create_tables(engine:sqlalchemy.engine.Engine, clear_existing:bool=True):

    if clear_existing:
        for table_name in list_tables(engine):
            drop_table(engine, table_name)

    tables.AnnotationsKegg.__table__.create(bind=engine)
    tables.AnnotationsPfam.__table__.create(bind=engine)
    tables.Metadata.__table__.create(bind=engine)
    tables.Proteins.__table__.create(bind=engine)

    tables.AnnotationsKeggHistory.__table__.create(bind=engine)
    tables.AnnotationsPfamHistory.__table__.create(bind=engine)
    tables.MetadataHistory.__table__.create(bind=engine)
    tables.ProteinsHistory.__table__.create(bind=engine)


engine = sqlalchemy.create_engine(get_database_url())
create_tables(engine)

