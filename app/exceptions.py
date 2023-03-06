class FindABugError(Exception):
    '''
    Exception raised when an error occurred with the Find-A-Bug backend.
    '''
    status_code = 500 # Indicates a server-side issue. 

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class FindABugQueryError(Exception):
    '''
    Exception raised when an error occurred when trying to parse a Query from
    the client-side. 
    '''
    status_code = 400 # Indicates a client-side issue. 
    
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
