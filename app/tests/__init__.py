'''
Tests to ensure client-to-database exchange is working. 
'''
import requests
import os
import nose
import json

# ip = '192.168.0.232:5000'
ip = '10.8.18.68:5000'


def test():
    '''
    '''
    
    for test_file in os.listdir('./tests/'):
        with open(f'./tests/{test_file}', 'r') as f:
            while f:
                # Remove the newline and trailing whitespace from the URL and
                # expected results. 
                url = f.readline().rstrip()
                exp = f.readline().rstrip()
                f.readline() # Skip the intermediate blank lines. 

                # Sometimes hits the end of the file and throws a json decoding
                # error. 
                try:
                    exp = json.loads(exp) 
                except:
                    break

                # NOTE: What does yield do?
                yield check_query, url, exp

def check_query(url, exp):
    '''
    '''
    res = requests.get(f'http://{ip}{url}').json()
    # Sometimes things get returned in different orders. Need to
    # compare dictionary contents. 
    for x, y in zip(exp, res):
             for k in x.keys():
                 if y[k] != x[k]:
                     assert False
    assert True

                     

nose.main()
