'''
Front-end API for accessing the genome database. All of these functions should
return pandas DataFrame objects with the requested information. 
'''
import requests

host = 'microbes.gps.caltech.edu:8000'


# TODO: Some kind of check function which makes sure all the specified options
# are OK. Not sure if I should put it in this file, or a module closer to the
# back end. Maybe both.

__operator_map = {'=':'eq', '>':'gt', '<':'lt', '<=':'le', '>=':'ge'}

def __where_value_to_url_string(where_value):
    '''
    '''
    if type(where_value) == tuple:
        operator, filter_ = where_value
    else: # Will be a float, integer, str, etc. 
        # In this case, assume the specified string is something to match
        # against (case 1).
        operator, filter_ = 'eq', where_value
    
    # Make sure the operator is in the correct form for the URL.
    operator = __operator_map.get(operator, operator)

    return f'{operator};{str(filter_)}'


def get(fields, where={}, verbose=True):
    '''

    args:
        : fields (str or list): Either a single field or a list of fields for
            which to retrieve information. 
    kwargs:
        : where (dict): Specifies search options. Some format options for the
            key, value pairs are as follows:
            (1) 'ko':'KO123'
            (2) 'ko':['KO123', 'KO456'] retrieves all fields where the KO group
              matches EITHER of the specified groups. 
            (3) 'threshold':('>', 500)
            (4) 'threshold':[('>', 500), ('<', 1000)]
    '''
    # If multiple fields are specified, join as a list. 
    if type(fields) == list:
        fields = '+'.join(fields)
    
    # Convert the specified options into a URL string. 
    options = [] 
    for key in where.keys():
        if type(where[key]) == list:
            for value in where[key]:
                s = __where_value_to_url_string(value)
                options.append(f'{key}=' + s)
        else: # If only one thing is specified.
            s = __where_value_to_url_string(where[key])
            options.append(f'{key}=' + s)
    # Join the options according to URL format.
    options = '+'.join(options)
    
    url = f'http://{host}/{fields}/{options}'
    try:
        # Does this return a string or a JSON something?
        result = requests.get(url).json()
    except requests.RequestsJSONDecodeError:
        result = str(result.get(url))
    
    if verbose:
        print(f'GET {url}')

    return result

def get_genes_with_score_over_threshold(annotation='kegg'):
    '''
    '''
    pass


def get_genes_in_ko_group(ko):
    '''
    '''
    pass
