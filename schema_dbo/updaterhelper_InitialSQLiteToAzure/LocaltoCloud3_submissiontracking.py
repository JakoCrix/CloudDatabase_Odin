# Admin
from Helper.Connections_Database import *
import pandas as pd
from sqlalchemy import create_engine
from urllib import parse
from datetime import datetime
from dateutil import tz

conn_odin_str, conn_odin_obj= connect_to_odinprod()


# SQL DB Extraction
conn_sqlite_object= connect_to_sqlite()
SubmissionTracking_Raw = pd.read_sql_query("""SELECT * FROM Submission_Tracking""",
                                           conn_sqlite_object)
SubmissionTracking1 = SubmissionTracking_Raw.copy()

# %% Processing
# -Subreddit Processing
Temp_Subreddit= pd.read_sql_query("SELECT idsubreddit, idsubreddit_reddit FROM subreddit_info",
                                  conn_odin_obj)

SubmissionTracking1_Added= pd.merge(left=SubmissionTracking1, right= Temp_Subreddit, how="left",
                                    left_on= "ID_Subreddit", right_on= "idsubreddit_reddit")
SubmissionTracking1_Added.rename(columns= {"idsubreddit":"ID_Subreddit_FK"}, inplace=True)
SubmissionTracking2= SubmissionTracking1_Added.copy()

# -Submission Processing
Temp_Submission= pd.read_sql_query("SELECT idsubmission, idsubmission_reddit FROM submission_info",
                                   conn_odin_obj)

SubmissionTracking2_Added= pd.merge(left=SubmissionTracking2, right= Temp_Submission, how="left",
                                    left_on= "ID_Submission", right_on= "idsubmission_reddit")
SubmissionTracking2_Added.rename(columns= {"idsubmission":"ID_Submission_FK"}, inplace=True)
SubmissionTracking3= SubmissionTracking2_Added.copy()


# -Additional Processing
SubmissionTracking3["Stickied"]= 0

def tempfunc_dtconversion_AUDtoUTC(Daterow):
    # Daterow= SubmissionTracking3["LastFetched"][1000]
    DateObj= datetime.strptime(Daterow, "%Y-%m-%d %H:%M")
    DateObj = DateObj.replace(tzinfo=tz.gettz('Australia/Victoria')) 
    DateObj=DateObj.astimezone(tz.gettz('UTC'))
    return(DateObj.strftime("%Y-%m-%d %H:%M:%S"))

SubmissionTracking3["LastFetched"]=SubmissionTracking3.apply(lambda row:tempfunc_dtconversion_AUDtoUTC(row["LastFetched"]), axis= 1)

SubmissionTracking4= SubmissionTracking3[['ID_Submission_FK','ID_Subreddit_FK', 'LastFetched',
                                          'NumComments', 'Score','UpvoteRatio',
                                          "Stickied",'IsClosed']].copy()
SubmissionTracking4.columns=["idsubmission", "idsubreddit", "lastfetched",
                              "numcomments", "score", "upvoteratio",
                              "stickied", "isclose"]

SubmissionTrackingFinal= SubmissionTracking4.copy()

# %% InsertionProcessing
conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()
params = parse.quote_plus(conn_odin_str)
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params), fast_executemany=True)

SubmissionTrackingFinal.to_sql(name="submissiontracking", con=engine, chunksize=100000, index= False,if_exists="append")

cursor.commit()
conn_odin_obj.close()

