# Admin
from Helper.Connections import *
import pandas as pd
from sqlalchemy import create_engine
from urllib import parse

import time

conn_odin_str, conn_odin_obj= connect_to_odinprod()


# SQL DB Extraction
conn_sqlite_object= connect_to_sqlite()
CommentInformation_Raw = pd.read_sql_query("""SELECT * FROM Comment_Information""", conn_sqlite_object)
Comment_Raw = pd.read_sql_query("""SELECT * FROM Comment""", conn_sqlite_object)

CommentsInfo_Raw= pd.merge(left=CommentInformation_Raw, right=Comment_Raw, how="left",
                           left_on="ID_Comment", right_on="ID_Comment")

CommentsInfo_Raw2=CommentsInfo_Raw.sort_values('created_utc', ascending=False).drop_duplicates('ID_Comment').reset_index(drop=True)

CommentsInfo1= CommentsInfo_Raw2.copy()


# %% Processing
CommentsInfo1["idcomment"]= range(1, len(CommentsInfo1)+1)
CommentsInfo1["initialreplynode"]= CommentsInfo1["ID_Submission"] == CommentsInfo1["ID_ParentID"]

# -Subreddit Processing
Temp_Submission = pd.read_sql_query("""SELECT ID_Subreddit, ID_Submission FROM Submission_Tracking""", conn_sqlite_object)
Temp_Submission= Temp_Submission.drop_duplicates()
Temp_Subreddit= pd.read_sql_query("SELECT idsubreddit, idsubreddit_reddit FROM subreddit_info", conn_odin_obj)
Temp_SubmSubreddit= pd.merge(left=Temp_Submission, right=Temp_Subreddit, how="left",
                             left_on="ID_Subreddit", right_on= "idsubreddit_reddit")
Temp_SubmSubreddit2= Temp_SubmSubreddit[["ID_Submission", "idsubreddit"]]

CommentsInfo1_Added= pd.merge(left= CommentsInfo1, right= Temp_SubmSubreddit2, how="left",
                              left_on="ID_Submission", right_on="ID_Submission")
CommentsInfo1_Added.rename(columns= {"idsubreddit":"ID_Subreddit_FK"}, inplace=True)

CommentsInfo2= CommentsInfo1_Added.copy()

# -Parent Processing
temp_parents= CommentsInfo2[["idcomment", "ID_Comment"]].copy()
temp_parents.rename(columns= {"idcomment":"idparent",
                              "ID_Comment":"idparent_reddit"}, inplace=True)

CommentsInfo3= pd.merge(left=CommentsInfo2, right= temp_parents, how= "left",
                        left_on= "ID_ParentID", right_on= "idparent_reddit")

# - Submission Processing
Temp_Submission= pd.read_sql_query("SELECT idsubmission, idsubmission_reddit FROM submission_info",conn_odin_obj)

CommentsInfo3_Added= pd.merge(left=CommentsInfo3, right= Temp_Submission, how="left",
                                    left_on= "ID_Submission", right_on= "idsubmission_reddit")
CommentsInfo3_Added.rename(columns= {"idsubmission":"ID_Submission_FK"}, inplace=True)

CommentsInfo4= CommentsInfo3_Added.copy()


# Consolidating
CommentsInfo4.dtypes
CommentsInfo5= CommentsInfo4[["idcomment", "ID_Comment", "ID_Submission_FK","ID_Subreddit_FK",
                              "initialreplynode", "body", "idparent",
                              "idparent_reddit", "created_utc"]].copy()
CommentsInfo5.columns= ["idcomment", "idcomment_reddit", "idsubmission", "idsubreddit",
                        "initialreplynode", "body", "idparent",
                        "idparent_reddit", "createdatetime"]


CommentsInfo_Final= CommentsInfo5.copy()
CommentsInfo_Final.dtypes

del [CommentsInfo_Raw, CommentsInfo_Raw2,
     CommentsInfo1, CommentsInfo2, CommentsInfo3, CommentsInfo4, CommentsInfo5]
del [Comment_Raw, CommentsInfo1_Added, CommentsInfo3_Added]

# %% Insertion
start_time = time.time()

conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()
params = parse.quote_plus(conn_odin_str)
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params), fast_executemany=True)

# Splitting into
len(CommentsInfo_Final)/1000000
CommentsInfo_Final[1:1000000].to_sql(name="comment_info", con=engine, chunksize=1000, index= False,if_exists="append")
CommentsInfo_Final[1000001:2000000].to_sql(name="comment_info", con=engine, chunksize=5000, index= False,if_exists="append")
CommentsInfo_Final[2000001:3000000].to_sql(name="comment_info", con=engine, chunksize=10000, index= False,if_exists="append")
CommentsInfo_Final[3000001:4000000].to_sql(name="comment_info", con=engine, chunksize=20000, index= False,if_exists="append")
CommentsInfo_Final[4000001:].to_sql(name="comment_info", con=engine, chunksize=50000, index= False,if_exists="append")

cursor.commit()
conn_odin_obj.close()
print("My program took", time.time() - start_time, "to run")
