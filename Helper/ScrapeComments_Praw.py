# %% Admin
from datetime import datetime
import pandas as pd
import time
import re

from Helper.Connections_Database import connect_to_reddit


# %% Scraping Functions
def PrawScraper_viaSubmission(SubmissionID_str, redditconnect_ptawobj):
    # SubmissionID_str = "ly92v2"; conn_reddit_object= connect_to_reddit()

    # Scraping-Connection
    submission = redditobj_praw.submission(id=SubmissionID_str)
    print("Scraping submission {}: '{}'".format(submission.id,submission.title))
    StartTime = datetime.now()
    submission.comments.replace_more(limit=100)
    print("_Submission object refreshed taking: {} seconds".format((datetime.now()- StartTime).total_seconds()))

    # Scraping- Scraping Process
    tempself_CurrentUTC= int(time.time())
    tempself_SubmissionID= SubmissionID_str

    Temp_data= []
    for comment in submission.comments.list():
        if comment.body == "[deleted]":
            pass
        else:
            Temp_data.append([
                comment.id, re.sub("^t._", "", comment.parent_id), tempself_SubmissionID,
                str(comment.author), comment.body, comment.created_utc,
                tempself_CurrentUTC, comment.score, comment.stickied
            ])

    print("_Scraping complete, extracted {} of {} comments".format(len(Temp_data), submission.num_comments))

    # Processing
    df = pd.DataFrame(Temp_data, columns=['Comment_ID', 'Comment_IDParent','Comment_IDSubmission',
                                          'Comment_Author', 'Comment_Body','Comment_UTCCreationTime',
                                          'Comment_UTCFetchTime', 'Comment_score', "Comment_Stickied"])

    df["Comment_UTCCreationTime"]= df["Comment_UTCCreationTime"].apply(datetime.utcfromtimestamp)
    df["Comment_UTCFetchTime"]= df["Comment_UTCFetchTime"].apply(datetime.utcfromtimestamp)

    # Returning
    df_Final=df

    return(df_Final)

