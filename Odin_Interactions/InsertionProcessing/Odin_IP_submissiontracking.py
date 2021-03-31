# %% Admin
from datetime import datetime
import pandas as pd
from Helper.Connections import *
# from Odin_Interactions.Extraction.ScrapeSubmissions import *

# Initial Processing
def IP_submissiontracking_reshape(Submission_Raw_Df, conn_Object):
    # deleteme, conn_Object= connect_to_odinprod()
    # Submission_Raw_Df= pd.read_csv("C:\\Users\\Andrew\\Documents\\GitHub\\CloudDatabase_Odin\\Odin_Interactions\\Insertion_Trial\\Submission_20210329.csv")

    # Extracting the relevant keys
    Temp_EarliestSubmission = datetime.utcfromtimestamp(int(min(Submission_Raw_Df.Submission_UTCCreationTime))).strftime('%Y-%m-%d %H:%M:%S')
    Temp_submissioninfo= pd.read_sql_query("SELECT idsubmission, idsubmission_reddit FROM submission_info "
                                         "WHERE createdatetime >= '{}'".format(Temp_EarliestSubmission), conn_Object)
    Temp_subredditinfo= pd.read_sql_query("SELECT idsubreddit, idsubreddit_reddit FROM subreddit_info", conn_Object)

    Submission_Raw_Df2_FK1= pd.merge(left= Submission_Raw_Df, right= Temp_submissioninfo, how="left",
                                 left_on="Submission_ID", right_on="idsubmission_reddit")
    Submission_Raw_Df2_FK1andFK2= pd.merge(left= Submission_Raw_Df2_FK1, right= Temp_subredditinfo, how="left",
                                 left_on="Subreddit_ID", right_on="idsubreddit_reddit")
    Submission_Raw_Df2= Submission_Raw_Df2_FK1andFK2.copy()
    Submission_Raw_Df2["isclose"]=0
    Submission_Raw_Df2["Submission_Stickied"]= Submission_Raw_Df2["Submission_Stickied"].astype(int)

    Submission_Raw_Df2["lastfetched"] = datetime.utcfromtimestamp(int(Submission_Raw_Df2["Submission_UTCFetchTime"].unique())).strftime('%Y-%m-%d %H:%M:%S')

    # Processing the final submissions
    Submission_Raw_Df3= Submission_Raw_Df2[["idsubmission","idsubreddit",
                                           "lastfetched", "Submission_NumComments", "Submission_Score", "Submission_UpvoteRatio","Submission_Stickied", "isclose"]]
    Submission_Raw_Df3.columns= ["idsubmission","idsubreddit",
                                 "lastfetched", "numcomments", "score", "upvoteratio", "stickied", "isclose"]

    processed_SubmissionTracking= Submission_Raw_Df3

    return processed_SubmissionTracking

# Function 1
# This function works towards removing all submissions that have already been closed.
def IP_submissiontracking_exclusions(Submission_Raw_Df, conn_Object):
    # deleteme, conn_Object= connect_to_odinprod()
    # Submission_Raw_Df= Insertme_SubmissionTracking
    Processing1_1 = Submission_Raw_Df.copy()

    # Extracting Closed Submissions
    Temp_submissioninfo = pd.read_sql_query("SELECT idsubmission FROM submissiontracking WHERE isclose=1 group by idsubmission", conn_Object)
    DontExtract_List = Temp_submissioninfo["idsubmission"].tolist()
    Processing1_1["isclose_Updated"] = Processing1_1["idsubmission"].isin(DontExtract_List).astype(int)

    # Finalizing and Returning
    Processing1_Final = Processing1_1[['idsubmission', 'idsubreddit', 'lastfetched', 'numcomments', 'score',
                                       'upvoteratio', 'stickied', 'isclose_Updated']].copy()
    Processing1_Final.rename(columns={'isclose_Updated': 'isclose'}, inplace=True)

    print("Processing 1 Complete- Excluding Closed Submissions:" +
          "\n- {} of {} submissions has already been closed".format(sum(Processing1_1["isclose"]),len(Processing1_1)) +
          "\n- {} submissions to process ".format(len(Processing1_Final) - sum(Processing1_Final["isclose"]))
          )

    return Processing1_Final


# Function 2
def IP_submissiontracking_PotentialClosures(Submission_Raw_Df, conn_Object):
    # deleteme, conn_Object= connect_to_odinprod()
    # Submission_Raw_Df= Insertme_SubmissionTracking2

    Processing2_1 = Submission_Raw_Df.copy()
    Processing2_1["DatabaseExistence"]= "New"

    # Extracting Submissions
    print("Processing 2 Started- Modifying Potential Closures IsClosed:")
    Temp_RedditSubmissions= "'"+ "','".join(str(e) for e in Processing2_1["idsubmission"].tolist())+ "'"
    TempSQL_IsCloseModifier = "SELECT * , 'Exists' as DatabaseExistence FROM submissiontracking WHERE idsubmission IN ({})""".format(Temp_RedditSubmissions)
    PotentialSubmissionsToClose = pd.read_sql_query(TempSQL_IsCloseModifier, conn_Object)

    # %% Looping
    Processing2_2 = pd.concat([PotentialSubmissionsToClose, Processing2_1]).reset_index(drop=True)
    Temp_LoopList = Processing2_2["idsubmission"].unique().tolist()

    Processing2_3 = pd.DataFrame()

    for Temp_SubmissionID_Indice in range(len(Temp_LoopList)):
        if Temp_SubmissionID_Indice % 250 == 0:
            print("-- Looping and Checking {} of {} submissions.".format(Temp_SubmissionID_Indice, len(Temp_LoopList)))

        Temp_SubmissionID = Temp_LoopList[Temp_SubmissionID_Indice]

        # Temp_SubmissionID= "jgkp92"
        Temp_Df = Processing2_2[Processing2_2["idsubmission"] == Temp_SubmissionID].copy()
        Temp_Df["Prcs2_WillClose"] = 0
        # Temp_Df[["Submission_ID", "Submission_LastFetched","Submission_NumComments"]]

        if len(Temp_Df) <= 2:
            Processing2_3 = pd.concat([Processing2_3, Temp_Df])

        else:
            Temp_LastRow = Temp_Df[Temp_Df["numcomments"] ==
                                   (Temp_Df["numcomments"].max())].iloc[0]["lastfetched"]
            TimeDifference = pd.to_datetime(Temp_Df.iloc[-1]["lastfetched"]) - pd.to_datetime(Temp_LastRow)

            if TimeDifference > pd.Timedelta(days=3):
                Temp_Df.at[Temp_Df.index[-1], "Prcs2_WillClose"] = 1
            else:
                pass

            Processing2_3 = pd.concat([Processing2_3, Temp_Df])

    # Finalizing and Returning
    Processing2_4 = Processing2_3[Processing2_3["DatabaseExistence"] == "New"].copy()
    Processing2_4 = Processing2_4.reset_index(drop=True)

    # Printing Information
    Processing2_Final = Processing2_4[['idsubmission', 'idsubreddit', 'lastfetched', 'numcomments', 'score',
                                       'upvoteratio', 'stickied', 'Prcs2_WillClose']].copy()
    Processing2_Final.rename(columns={'Prcs2_WillClose': 'isclose'}, inplace=True)

    print("Processing 2 Complete- Modifying Potential Closures IsClosed:"+
          "\n-- {} submissions still open ".format(len(Submission_Raw_Df))+
          "\n-- {} of {} will be closed moving forward ".format(sum(Processing2_Final["isclose"]), len(Submission_Raw_Df))
          )

    return Processing2_Final













