import sqlalchemy
from app import tables
from utils import get_database_url

def create_tables(engine:sqlalchemy.engine.Engine):

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

