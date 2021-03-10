# Admin
from Helper.Connections import *
import pandas as pd
from datetime import datetime

conn_sqlite_object= connect_to_sqlite()
conn_reddit_object= connect_to_reddit()
conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()

# SQL DB Datagrab
tempquery1_submissiontracking= """SELECT * FROM Submission_Tracking"""
SubmissionTracking_Raw = pd.read_sql_query(tempquery1_submissiontracking, conn_sqlite_object)
SubmissionTracking = SubmissionTracking_Raw.copy()

# %% Processing
# -Subreddit Processing
Temp_Subreddit= pd.read_sql_query("SELECT idsubreddit, idsubreddit_reddit FROM subreddit", conn_odin_obj)

SubmissionTracking2= pd.merge(left=SubmissionTracking, right= Temp_Subreddit, how="left",
                              left_on= "ID_Subreddit", right_on= "idsubreddit_reddit")
SubmissionTracking2

# TODO: continue here


tempquery3_subreddit= """SELECT * FROM Subreddit_Info"""
Subreddit = pd.read_sql_query(tempquery3_subreddit, conn_sqlite_object)
Subreddit = Subreddit.copy()


SubmissionTracking["ID_Subreddit"].unique()


# -Submission Datagrab
tempquery2_submission= """SELECT * FROM Submission_Info"""
Submission = pd.read_sql_query(tempquery2_submission, conn_sqlite_object)
Submission2 = Submission.copy()






SubmissionTracking2








SubmissionTracking2["idsubmission"]= range(1, len(SubmissionTracking2)+1)
SubmissionInfo2["originalcontent"]= False