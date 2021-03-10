# Admin
from Helper.Connections import *
import pandas as pd
from datetime import datetime

conn_sqlite_object= connect_to_sqlite()
conn_reddit_object= connect_to_reddit()
conn_odin_str, conn_odin_obj= connect_to_odinprod()


# %% SQL DB Datagrab
temp_query1= """SELECT * FROM Subreddit_Info"""
SubredditInfo = pd.read_sql_query(temp_query1, conn_sqlite_object)
SubredditInfo2 = SubredditInfo.copy()

# %% Processing
SubredditInfo2["idsubreddit"]= range(1, len(SubredditInfo2)+1)

def tempfunc_utctodatetime(subredditid):
    subreddit_obj=conn_reddit_object.subreddit(subredditid)
    subreddit_obj2= subreddit_obj.created_utc
    subreddit_obj3= datetime.utcfromtimestamp(int(subreddit_obj2)).strftime('%Y-%m-%d %H:%M:%S')

    return subreddit_obj3

SubredditInfo2["createdatetime"]= SubredditInfo2.apply(lambda row: tempfunc_utctodatetime(row['Name']),
                                                       axis=1)
SubredditInfo2["url"]= "https://www.reddit.com/r/"+SubredditInfo2["Name"]

SubredditInfo3= SubredditInfo2[["idsubreddit", "ID_Subreddit", "createdatetime", "Name", "url"]]
SubredditInfo3.columns= ["idsubreddit", "idsubreddit_reddit", "createdatetime", "title", "url"]
SubredditInfoFinal= SubredditInfo3.copy()

# %% Insertion
cursor= conn_odin_obj.cursor()

insert_sql= "INSERT INTO subreddit (idsubreddit, idsubreddit_reddit, createdatetime, title, url) "\
            "VALUES (?, ?, ?, ?, ?)"
records= SubredditInfoFinal.values.tolist()

cursor.executemany(insert_sql, records)
cursor.commit()

