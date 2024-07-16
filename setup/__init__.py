import sqlalchemy
from app import tables

def create_tables(engine:sqlalchemy.engine.Engine):

    tables.AnnotationsKegg.__table__.create(bind=engine)
    tables.AnnotationsPfam.__table__.create(bind=engine)
    tables.Metadata.__table__.create(bind=engine)
    tables.Proteins.__table__.create(bind=engine)

    tables.AnnotationsKeggHistory.__table__.create(bind=engine)
    tables.AnnotationsPfamHistory.__table__.create(bind=engine)
    tables.MetadataHistory.__table__.create(bind=engine)
    tables.ProteinsHistory.__table__.create(bind=engine)