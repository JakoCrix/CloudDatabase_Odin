# %% Admin
from datetime import datetime
import pandas as pd
import time

from Helper.Connections import connect_to_reddit

# %% Scrape Comments
SubmissionID_str = "ipp3e9"
conn_reddit_object= connect_to_reddit()
submission = conn_reddit_object.submission(id=SubmissionID_str)

print("_Scraping {} with title: '{}'".format(submission.id,submission.title))
StartTime = datetime.now()
submission.comments.replace_more(limit=100)
print("_Submission object refreshed taking: {} seconds".format((datetime.now()- StartTime).total_seconds()))

comments = submission.comments
temp_currentdatetimeutc= int(time.time())
temp_SubmissionID_str=SubmissionID_str

commentslist = [[comment.id, comment.parent, temp_SubmissionID_str,
                 str(comment.author), comment.created_utc, comment.body,
                 temp_currentdatetimeutc, comment.score, comment.stickied] for comment in comments]
df = pd.DataFrame(commentslist,
                  columns=['Comment_ID', 'Comment_IDParent','Comment_IDSubmission',
                           'Comment_Author', 'Comment_Body','Comment_UTCCreationTime',
                           'Comment_UTCFetchTime', 'Comment_score', "Comment_Stickied"])

print("_Scraping complete, extracted {} of {} comments".format(len(df),
                                                               submission.num_comments)
      )

# TODO: Think of cleaning the Dataframe before database insertion
