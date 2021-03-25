# Admin
from Helper.Connections import *
import pandas as pd
import datetime

from sqlalchemy import create_engine
from urllib import parse


# Odin DB Extraction
conn_odin_str, conn_odin_obj= connect_to_odinprod()
commenttracking_Raw= pd.read_sql_query("SELECT idcomment FROM comment_info", conn_odin_obj)
commenttracking1= commenttracking_Raw.copy()

# %% Processing
lastfetched= datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") #Check me
commenttracking1["lastfetched"]= lastfetched
commenttracking1["score"]= None
commenttracking1["stickied"]= None

commenttracking2= commenttracking1[["idcomment", "lastfetched", "score", "stickied"]]

CommentsInfo_Final= commenttracking2.copy()


# %% InsertionProcessing
conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()
params = parse.quote_plus(conn_odin_str)
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params), fast_executemany=True)


start_time = time.time()
CommentsInfo_Final.to_sql(name="commenttracking", con=engine, chunksize=100000, index= False, if_exists="append")
print("My program took", time.time() - start_time, "to run")


cursor.commit()
conn_odin_obj.close()