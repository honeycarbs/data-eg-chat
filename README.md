# data-eg-chat
Repo for any ccode that we might need



# Useful commands:

Ensure your environment has the DB_URI set (specific URI found in slack pinned msgs):
FOR MAC:
1. Add this to ~/.bash_profile or ~/.bashrc: export DB_URI="database_uri_here"
2. Note: if you add it to the command line rather than one of the bash folders then it will be cleared
when the terminal is exited. 

For a new cloned repo:
1. Create a virtual python environment, make sure its name is env for auto scheduling: 'python -m venv env'
2. Run the virtual env: 'source env/bin/activate'
3. While virtual env is running: 'pip install google-cloud-pubsub'


