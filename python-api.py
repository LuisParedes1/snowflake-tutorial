# Install snowflake
# pip install snowflake -U

# Define a connection to Snowflake
import os
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


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

logger.info("Connected to snowflakes")

# With a Root object created from your connection to Snowflake, you can access objects 
# and methods of the Snowflake Python APIs
# You use the Root object to interact with Snowflake objects represented by the API.
tables = root.databases["PUBLIC_DATA"].schemas["PUBLIC"].tables
metadata = tables["COMPANY_METADATA"]

logger.info("Found table %s", metadata.name)

# Use the connector's cursor to execute a query
with connection.cursor() as cur:
    cur.execute("SELECT * FROM PUBLIC_DATA.PUBLIC.COMPANY_METADATA LIMIT 5")
    rows = cur.fetchall()
    logger.info("First 5 rows from COMPANY_METADATA: %s", rows)
