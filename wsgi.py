import sys
import site
import os
import mariadb
# # Path to the environment to activate. 
# env_path = '/home/prichter/.conda/envs/findabug/bin/python'
# 
# # NOTE: What does this do under-the-hood?
# with open(env_path) as f:
#     exec(f.read(), dict(__file__=env_path))
# 
# # NOTE: Not sure what this is really doing either.
# sys.stdout = sys.stderr
# 
# What is the addsitedir doing?
# site.addsitedir('/var/www/find-a-bug/lib/python3.6/site-packages')
sys.path.insert(0, '/home/prichter/find-a-bug/app')
sys.path.insert(0, '/home/prichter/find-a-bug/')

from app import app as application 

# Launch the Flask app when the file is called. 
if __name__ == '__main__':
    # app.run(debug=True)
    # NOTE: Still slightly confused by how host is working here. 
    # application.run(debug=True, port=8000, host='0.0.0.0')
    application.run(debug=True, port=8000, host='0.0.0.0')
