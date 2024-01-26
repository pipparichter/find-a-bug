# Find-A-Bug

The Find-A-Bug project is a web application which enables client access to an underlying SQL database hosted on a remote server. It supports a RESTful API, which handles queries to the underlying database. A Python API is also under development, and can be installed at [https://github.com/pipparichter/find-a-bug-api](https://github.com/pipparichter/find-a-bug-api). 

**NOTE:** In order to access the server, users must be on Caltech wifi or using a VPN.

## Database

## Request URL format

The Find-A-Bug web apps supports two classes of URL requests: specific queries of the underlying SQL database, and a general request for information about the database. The following URL returns information about the tables contained in the database, including column fields, sizes, and data types.

**This has not yet been implemented.**

[https://microbes.gps.caltech.edu/info](https://microbes.gps.caltech.edu/info)


### Querying

The Find-A-Bug web app accepts and parses URL strings of the following format. It returns 100 matching results by default. Pagination is required to access the entire set of matching results id this set exceeds 100. 

`https://microbes.gps.caltech.edu/<resource>?<field>=<value>&<field>=<value>...#<page>`

1. `<resource>`: One of `'annotations`, `'metadata'`, or `'sequences'`. This specifies the table being accessed. 
2. `<field>`: The name of a column in a table on which to apply a query specification. This field does not need to be in the table specified by `<resource>`
3. `<value>`: The condition to apply to the specified column. This can be of the form `'{operator}{x}` where operator is one of `<`, `>`, or `=`. If no operator is specified, then the equals operator is implied.
4. `<page>`: The page of results to access. Each page is, at most, 100 results matching the specified criteria. 

