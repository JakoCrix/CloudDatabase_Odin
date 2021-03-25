# %% Admin
from datetime import datetime
import pandas as pd
from Helper.Connections import *
from Odin_Interactions.Extraction.ScrapeSubmissions import *

def InsertionProcessing_subredditinfo(Submission_Raw_Df, conn_Object, conn_RedditObj):
    pass

# %% Admin
deleteme, conn_Object= connect_to_odinprod()
conn_RedditObj= connect_to_reddit()
Submission_Raw_Df= ScrapeReddit_SubmissionviaSubreddit(Subreddit_Name="stocks", conn_reddit_object= conn_RedditObj, MinimumComments=25)

# %% Function Starts
Submission_Raw_Df.dtypes
CurrentTable= pd.read_sql_query("SELECT idsubmission, idsubmission_reddit FROM submission_info", conn_Object)
CurrentTable

processed_SRinfo= pd.DataFrame()

NewSubmissions.dtypes
NewSubmissions= Submission_Raw_Df.[Submission_Raw_Df["Submission_ID"].isin(CurrentTable["idsubmission_reddit"].tolist())==False].copy()
NewSubmissions= NewSubmissions["Submission_ID", "Submission_Title", ]


for SubredditsofInterest in All_Subreddits:
    # SubredditsofInterest= All_Subreddits[0]
    if SubredditsofInterest not in CurrentTable.title.to_list():
        # InsertionProcessing Prep
        Insertion_Df= pd.DataFrame(data={"idsubreddit": [max(CurrentTable.idsubreddit)+1],
                           "idsubreddit_reddit":[SubredditsofInterest],
                           "createdatetime":[datetime.utcfromtimestamp(
                               int(conn_RedditObj.subreddit(SubredditsofInterest).created_utc)).strftime(
                               '%Y-%m-%d %H:%M:%S')],
                           "title":[SubredditsofInterest],
                           "url":["https://www.reddit.com/r/" + SubredditsofInterest]
                           })

    processed_SRinfo= processed_SRinfo.append(Insertion_Df)