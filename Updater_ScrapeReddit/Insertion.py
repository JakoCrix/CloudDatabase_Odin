########################################################################
# %% Admin
########################################################################
from sqlalchemy import create_engine
from urllib import parse
import pandas as pd
import sys
sys.path.append('C:\\Users\\Andrew\\Documents\\GitHub\\CloudDatabase_Odin')


# Helper Files
from Helper.Connections_Database import *
from Helper.Connections_Scraping import *

from Helper.ScrapePRAW_Comments import *
from Helper.ScrapePRAW_Submissions import *

from Updater_ScrapeReddit.Helper_InsertionProcessing.IP_submissioninfo import IP_submissioninfo
from Updater_ScrapeReddit.Helper_InsertionProcessing.IP_subredditinfo import IP_subredditinfo
from Updater_ScrapeReddit.Helper_InsertionProcessing.IP_submissiontracking import *
from Updater_ScrapeReddit.Helper_InsertionProcessing.IP_commentinfo_commenttracking import *


# Connections
conn_reddit_object = redditconnect_PRAW()
conn_odin_str, conn_odin_obj = connect_to_odinprod()
cursor = conn_odin_obj.cursor()
params = parse.quote_plus(conn_odin_str)
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params), fast_executemany=True)

# Admin Specifications
Temp_SubredditList = ["stocks",
                      "investing",
                      "wallstreetbets",
                      "Stock_Picks",
                      "pennystocks"]

########################################################################
# %% Submission Extraction
########################################################################
Submission_Df = pd.DataFrame()

for Subreddit in Temp_SubredditList:
    # Subreddit= "stocks"
    SubmissionIndiv_Df= ScrapeReddit_SubmissionviaSubreddit(Subreddit_Name=Subreddit, conn_reddit_object=conn_reddit_object, MinimumComments=20)
    Submission_Df = pd.concat([Submission_Df, SubmissionIndiv_Df])


########################################################################
# %% Database Insertion- LHS
########################################################################
# SubmissionInfo Insertion
Insertme_SubmissionInfo= IP_submissioninfo(Submission_Df, conn_odin_obj)
Insertme_SubmissionInfo.to_sql(name="submission_info", con=engine, chunksize=100000, index=False, if_exists="append")
cursor.commit()

# Subreddit Insertion
Insertme_SubredditInfo=IP_subredditinfo(Submission_Df, conn_odin_obj, conn_reddit_object)
Insertme_SubredditInfo.to_sql(name="subreddit_info", con=engine, chunksize=100000, index= False, if_exists="append")
cursor.commit()

# Submission Insertion
Insertme_SubmissionTracking = IP_submissiontracking_reshape(Submission_Df, conn_odin_obj)
Insertme_SubmissionTracking2 = IP_submissiontracking_exclusions(Insertme_SubmissionTracking, conn_odin_obj)
Insertme_SubmissionTracking3 = IP_submissiontracking_PotentialClosures(Insertme_SubmissionTracking2, conn_odin_obj)
Insertme_SubmissionTracking3.to_sql(name="submissiontracking", con=engine, chunksize=100000, index=False, if_exists="append")
cursor.commit()

SubmissionsToInsert_Raw = Insertme_SubmissionTracking3.copy()

####################################
# %% RHS Insertion
####################################
# Identify submissions
temp_script_submissions = ", ".join(str(e) for e in SubmissionsToInsert_Raw["idsubmission"].tolist())
temp_script1 = "SELECT idsubmission, idsubmission_reddit FROM submission_info WHERE idsubmission in ({})".format(temp_script_submissions)
SubmissionsToInsert = pd.read_sql_query(temp_script1, conn_odin_obj)

# Identify current comments
temp_script2 = "SELECT idcomment, idcomment_reddit, idsubmission FROM comment_info WHERE idsubmission in ({})".format(temp_script_submissions)
CommentsinDB = pd.read_sql_query(temp_script2, conn_odin_obj)


# Database Insertion
for SubmissionIndex in range(len(SubmissionsToInsert)):
    # SubmissionIndex=0
    print("____" * 20)

    # Extraction
    print("Inserting Comments for Submission {} of {}".format(SubmissionIndex + 1, len(SubmissionsToInsert)))
    try:
        Temp_Comments = ScrapeReddit_CommentsviaSubmission(SubmissionID_str= SubmissionsToInsert["idsubmission_reddit"][SubmissionIndex],
                                                           conn_reddit_object= conn_reddit_object)
    except TimeoutError:
        print("_Caught a timeout! Sleeping for 10 seconds before retrying. ")
        time.sleep(10)
        Temp_Comments = ScrapeReddit_CommentsviaSubmission(SubmissionID_str= SubmissionsToInsert["idsubmission_reddit"][SubmissionIndex],
                                                           conn_reddit_object= conn_reddit_object)
    except Exception:
        print("_Error with extraction, logging as an error and passing. ")
        pass

    # Insertion Processing
    if Temp_Comments.empty:
        pass
    else:
        # comment_info
        Insertme_CommentInfo, Insertme_Commenttracking = IP_commentsinfo_commenttracking(
            Comments_Insertion_Df=Temp_Comments,
            Comments_Current_Df=CommentsinDB,
            conn_DB_Object=conn_odin_obj)

        Insertme_CommentInfo.to_sql(name="comment_info", con=engine, chunksize=100000, index=False, if_exists="append")
        Insertme_Commenttracking.to_sql(name="commenttracking", con=engine, chunksize=100000, index=False, if_exists="append")
        cursor.commit()
