# Install snowflake
# pip install snowflake -U

# Define a connection to Snowflake
import os
from dotenv import load_dotenv
load_dotenv()

CONNECTION_PARAMETERS = {
    "account": os.environ["snowflake_account"],
    "user": os.environ["snowflake_user"],
    "password": os.environ["snowflake_password"],
    "role": os.environ["role"],
    "database": os.environ["database"],
    "warehouse": os.environ["warehouse"],
    "schema": os.environ["schema"],
}

# With the connection, you can create a Root object for access to resources modeled by the API.
from snowflake.connector import connect
from snowflake.core import Root

connection = connect(**CONNECTION_PARAMETERS)
root = Root(connection)

# With a Root object created from your connection to Snowflake, you can access objects 
# and methods of the Snowflake Python APIs
# You use the Root object to interact with Snowflake objects represented by the API.
tasks = root.databases["mydb"].schemas["myschema"].tasks
mytask = tasks["mytask"]
mytask.resume()