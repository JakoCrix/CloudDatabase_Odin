# Admin
from Helper.Connections import *
import pandas as pd
from datetime import datetime


# %% SQL DB Extraction
conn_sqlite_object= connect_to_sqlite()
SubredditInfo_Raw = pd.read_sql_query("""SELECT * FROM Subreddit_Info""",
                                  conn_sqlite_object)
SubredditInfo1 = SubredditInfo_Raw.copy()

# %% Processing
conn_reddit_object= connect_to_reddit()
SubredditInfo1["idsubreddit"]= range(1, len(SubredditInfo1)+1)

def tempfunc_utctodatetime(subredditid):
    subreddit_obj=conn_reddit_object.subreddit(subredditid)
    subreddit_obj2= subreddit_obj.created_utc
    subreddit_obj3= datetime.utcfromtimestamp(int(subreddit_obj2)).strftime('%Y-%m-%d %H:%M:%S')
    return subreddit_obj3

SubredditInfo1["createdatetime"]= SubredditInfo1.apply(lambda row: tempfunc_utctodatetime(row['Name']),axis=1)
SubredditInfo1["url"]= "https://www.reddit.com/r/"+SubredditInfo1["Name"]

SubredditInfo2= SubredditInfo1[["idsubreddit", "ID_Subreddit", "createdatetime", "Name", "url"]]
SubredditInfo2.columns= ["idsubreddit", "idsubreddit_reddit", "createdatetime", "title", "url"]
SubredditInfo_Final= SubredditInfo2.copy()

# %% InsertionProcessing
conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()

insert_sql= "INSERT INTO subreddit_info (idsubreddit, idsubreddit_reddit, createdatetime, title, url) "\
            "VALUES (?, ?, ?, ?, ?)"
records= SubredditInfo_Final.values.tolist()

cursor.executemany(insert_sql, records)
cursor.commit()
conn_odin_obj.close()