# %% Admin
from datetime import datetime
import pandas as pd
# from Helper.Connections_Database import *

# Function Starts
def IP_subredditinfo(Submission_Raw_Df,
                     conn_Object,
                     conn_RedditObj):
    # deleteme, conn_Object= connect_to_odinprod(); conn_RedditObj= connect_to_reddit()
    # Submission_Raw_Df= pd.read_csv("C:\\Users\\Andrew\\Documents\\GitHub\\CloudDatabase_Odin\\Odin_Interactions\\Insertion_Trial\\Submission_20210329.csv")

    # Extraction
    All_Subreddits= Submission_Raw_Df["Subreddit_Name"].unique()
    CurrentTable= pd.read_sql_query("SELECT idsubreddit, idsubreddit_reddit, title FROM subreddit_info", conn_Object)

    processed_SRinfo= pd.DataFrame()

    for SubredditsofInterest in All_Subreddits:
        # SubredditsofInterest= All_Subreddits[0]
        if SubredditsofInterest not in CurrentTable.title.to_list():
            # InsertionProcessing Prep
            Insertion_Df= pd.DataFrame(data={
                "idsubreddit": [max(CurrentTable.idsubreddit)+1],
                "idsubreddit_reddit":[
                    conn_RedditObj.subreddit(SubredditsofInterest).id],
                "createdatetime":[datetime.utcfromtimestamp(int(
                    conn_RedditObj.subreddit(SubredditsofInterest).created_utc)).strftime('%Y-%m-%d %H:%M:%S')],
                "title":[SubredditsofInterest],
                "url":["https://www.reddit.com/r/" + SubredditsofInterest]
            })

            processed_SRinfo= processed_SRinfo.append(Insertion_Df)

    return(processed_SRinfo)





