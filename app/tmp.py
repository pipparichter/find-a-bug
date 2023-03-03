from backend import FindABug
from sqlalchemy import create_engine

url = 'mariadb+mariadbconnector://root:Doledi7-Bebyno2@localhost/findabug'
engine = create_engine(url, echo=True)

fab = FindABug(engine)

x = fab.query_database(['gene_name'], options = {'ko':('eq', 'K02956')})

for row in x:
    print(row)
