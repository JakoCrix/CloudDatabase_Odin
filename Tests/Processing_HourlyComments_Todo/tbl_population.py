# %% Admin
from Helper.Connections_Database import connect_to_odinprod
import pandas as pd
from sqlalchemy import create_engine
from urllib import parse

# %% Processing
conn_odin_str, conn_odin_obj= connect_to_odinprod()
Comments_Raw = pd.read_sql_query("SELECT idcomment_reddit, idsubreddit, createdatetime FROM comment_info", conn_odin_obj)

Comments2= Comments_Raw.copy()
Comments2["createdatetime"] = pd.to_datetime(Comments2["createdatetime"])
Comments2["createdatetime_round"]= Comments2['createdatetime'].dt.floor('h')
Comments3= Comments2.groupby(["createdatetime_round", "idsubreddit"], as_index=False)["idcomment_reddit"].count()
Comments3= Comments3.sort_values(["idsubreddit", "createdatetime_round"]).reset_index(drop=True)

Comments_Final= Comments3.copy()
Comments_Final.columns=["datetime_hour", "idsubreddit", "sumcomments_hour"]

# %% Initial Table updaterhelper_InitialSQLiteToAzure
cursor = conn_odin_obj.cursor()
params = parse.quote_plus(conn_odin_str)
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params), fast_executemany=True)

Comments_Final.to_sql(name="reddit_sumcomments", schema="viz",
                      con=engine, chunksize=10000, index=False, if_exists="append")

