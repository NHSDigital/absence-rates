import sqlalchemy as sa
import pandas as pd
import logging


logger = logging.getLogger(__name__)

def get_df_from_sql(database, query) -> pd.DataFrame:
    """
    Uses sqlalchemy to connect to the NHSD server and database with the help
    of mssql and pyodbc packages

    Inputs:
        server: server name
        database: database name
        query: string containing a sql query

    Output:
        pandas Dataframe
    """
    conn = sa.create_engine(f"xxx", fast_executemany=True)
    conn.execution_options(autocommit=True)
    logger.info(f"Getting dataframe from SQL database {database}")
    logger.info(f"Running query:\n\n {query}")
    df = pd.read_sql_query(query, conn)
    return df

def execute_sql(database, query) -> None:
    """
    Uses sqlalchemy to connect to the NHSD SQL Server and executes a query assigned to that database.

    Inputs:
        database: database name
        query: string containing a sql query

    Output:
        Runs a SQL Server query
    """
    conn = sa.create_engine(f"xxx", fast_executemany=True)
    conn.execute(query)