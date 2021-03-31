# %% Admin
from datetime import datetime
import pandas as pd
from Helper.Connections import *
# from Odin_Interactions.Extraction.ScrapeSubmissions import *

# Function Starts
def IP_submissioninfo(Submission_Raw_Df, conn_Object):
    # deleteme, conn_Object= connect_to_odinprod(); conn_RedditObj= connect_to_reddit()
    # Submission_Raw_Df= pd.read_csv("C:\\Users\\Andrew\\Documents\\GitHub\\CloudDatabase_Odin\\Odin_Interactions\\Insertion_Trial\\Submission_20210329.csv")

    NewSubmissions= Submission_Raw_Df.copy()

    # Identifying New Submissions through "DropMe" column
    Temp_EarliestSubmission = datetime.utcfromtimestamp(
        int(min(Submission_Raw_Df.Submission_UTCCreationTime))).strftime('%Y-%m-%d %H:%M:%S')
    CurrentTable = pd.read_sql_query("SELECT idsubmission, idsubmission_reddit, createdatetime FROM submission_info "+
                                     "WHERE createdatetime >= '{}'".format(Temp_EarliestSubmission),
                                     conn_Object)

    NewSubmissions["DropMe"] = NewSubmissions["Submission_ID"].isin(CurrentTable["idsubmission_reddit"].tolist())
    NewSubmissions= NewSubmissions.sort_values("Submission_UTCCreationTime", ascending=True).reset_index(drop=True)

    # Processing into odin format
    NewSubmissions["idsubmission"] = range(max(CurrentTable.idsubmission) + 1, max(CurrentTable.idsubmission) + len(NewSubmissions) + 1)
    def tempfunc_utctodatetime(utcInt):
        utcdatetime = datetime.utcfromtimestamp(int(utcInt)).strftime('%Y-%m-%d %H:%M:%S')
        return utcdatetime
    NewSubmissions["createdatetime"] = NewSubmissions.apply(lambda row: tempfunc_utctodatetime(row['Submission_UTCCreationTime']), axis=1)
    NewSubmissions["Submission_OriginalContent"]= NewSubmissions["Submission_OriginalContent"].astype(int)


    # Finalizing and Returning
    NewSubmissions2 = NewSubmissions[["DropMe", "idsubmission", "Submission_ID", "Submission_Title", "Submission_Selftext",
                                      "createdatetime", "Submission_URL", "Submission_OriginalContent"]].copy()
    NewSubmissions2.columns = ["DropMe", "idsubmission", "idsubmission_reddit", "title", "submissiontext",
                               "createdatetime", "url", "originalcontent"]

    processed_submissioninfo= NewSubmissions2[NewSubmissions2.DropMe==False].copy()

    return processed_submissioninfo



