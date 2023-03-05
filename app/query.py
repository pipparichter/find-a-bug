'''
Class for managing client-side queries to the Find-A-Bug database (i.e. queries
received at the remote host from a url or the Find-A-Bug API.  
'''
from database import get_all_fields
from exceptions import QueryError

class Query():
    
    def __init__(self, url_query, url_options):
        
        self.options = Query.parse_options(url_options)
        self.query = url_query.split('+')

        # Check to make sure all query fields are OK. 
        fields_in_database = get_all_fields()
        fields_in_query = self.query + list(self.options.keys())
        for f in fields_in_query:
            if f not in fields_in_database:
                msg = f'{f} is not a field in the Find-A-Bug database.'
                raise QueryError(msg)

    def parse_options(url_options):
        '''
        Parse the string of options. String should be of the form
        field=value;operator+field=value;operator+....

        args:
            : url_options (string)
        returns:
            : options (dict)
        '''
        options = {}
        for option in url_options.split('+'):
            
            field = option.split('=')[0]
            filter_ = option.split('=')[1]
            operator, value = filter_.split(';')
                
            # The only types in the database (and I think all that there
            # will be) are floats, ints, and strings. 
            # TODO: There is probably a better way to handle this... 
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    # If neither of these two conversions work, make it a string. 
                    pass

            if field not in options:
                options[field] = [(operator, value)]
            else:
                options[field].append((operator, value))

        return options


