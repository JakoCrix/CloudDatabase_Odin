# %% Admin
from datetime import datetime
import pandas as pd
import time
import re

from Helper.Connections import connect_to_reddit


# %% Scraping Functions
def ScrapeReddit_CommentsviaSubmission(SubmissionID_str, conn_reddit_object):
    # SubmissionID_str = "ly92v2"; conn_reddit_object= connect_to_reddit()

    # Scraping-Connection
    submission = conn_reddit_object.submission(id=SubmissionID_str)
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

    df = pd.DataFrame(Temp_data, columns=['Comment_ID', 'Comment_IDParent','Comment_IDSubmission',
                                          'Comment_Author', 'Comment_Body','Comment_UTCCreationTime',
                                          'Comment_UTCFetchTime', 'Comment_score', "Comment_Stickied"])

    print("_Scraping complete, extracted {} of {} comments".format(len(df), submission.num_comments))

    return(df)

def ScrapeReddit_SubmissionviaSubreddit(Subreddit_Name, conn_reddit_object,MinimumComments=30):
    # Subreddit_Name="stocks"; conn_reddit_object= connect_to_reddit(); MinimumComments=25

    # Scraping-Connection
    subreddit = conn_reddit_object.subreddit(Subreddit_Name)
    AllPosts = subreddit.new(limit=1000)  # limit to 1000 submissions

    # Scraping- Scraping Process
    tempself_SubredditName = subreddit.display_name
    tempself_SubredditID = subreddit.id
    tempself_CurrentUTC = int(time.time())

    temp_SubmissionCount = 0
    temp_SubmissionHaveContent = 0
    temp_SubmissionLackContent = 0
    Temp_data = []

    for submission in AllPosts:
        # submission= conn_reddit_object.submission("j2gc7k")
        temp_SubmissionCount += 1

        try:
            if submission.num_comments >= MinimumComments:
                Temp_data.append([tempself_SubredditID, tempself_SubredditName,
                                  submission.id, submission.title, submission.url, submission.stickied, submission.created_utc,
                                  tempself_CurrentUTC, submission.num_comments, submission.score, submission.upvote_ratio
                                  ])
                # Tracking
                temp_SubmissionHaveContent += 1
                if temp_SubmissionCount % 100 == 0:
                    print("In subreddit {} of {} comments: {} has content and {} lacks content".format(Subreddit_Name,
                                                                                                       temp_SubmissionCount,
                                                                                                       temp_SubmissionHaveContent,
                                                                                                       temp_SubmissionLackContent))
            else:
                # Tracking
                temp_SubmissionLackContent += 1
                if temp_SubmissionCount % 100 == 0:
                    print("In subreddit {} of {} comments: {} has content and {} lacks content".format(Subreddit_Name,
                                                                                                       temp_SubmissionCount,
                                                                                                       temp_SubmissionHaveContent,
                                                                                                       temp_SubmissionLackContent))
        except:
            pass


    df = pd.DataFrame(Temp_data, columns=['Subreddit_ID', 'Subreddit_Name',
                                          'Submission_ID','Submission_Title','Submission_URL','Submission_Stickied','Submission_UTCCreationTime',
                                          'Submission_UTCFetchTime', 'Submission_NumComments', "Submission_Score",'Submission_UpvoteRatio'])

    return df

