##############################
# Admin
##############################
# Azure Libraries
import os
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import logging

import praw
import pyodbc
import time
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from urllib import parse
import pandas as pd
import re

##############################
# Helper Functions
##############################
# Connections
def connect_to_reddit():

    # Cloud Connection Information
    default_credential = DefaultAzureCredential()
    secret_client = SecretClient(
        vault_url="https://keyvaultforquant.vault.azure.net/",
        credential=default_credential
    )
    temp_prawcred_cliendid    = secret_client.get_secret(name="RedditConnect-clientid")
    temp_prawcred_clientsecret= secret_client.get_secret(name="RedditConnect-clientsecret")
    temp_prawcred_username    = secret_client.get_secret(name="RedditConnect-username")
    temp_prawcred_passwords   = secret_client.get_secret(name="RedditConnect-password")

    # Praw Object
    prawreddit_object= praw.Reddit(client_id=temp_prawcred_cliendid.value,
                                    client_secret=temp_prawcred_clientsecret.value,
                                    username=temp_prawcred_username.value, password=temp_prawcred_passwords.value,
                                    user_agent="prawtutorialv1")

    return prawreddit_object
def connect_to_odinprod():

    # Cloud Connection Information
    default_credential = DefaultAzureCredential()
    secret_client = SecretClient(
        vault_url="https://keyvaultforquant.vault.azure.net/",
        credential=default_credential
    )
    temp_odin_server = "quantserver.database.windows.net,1433"
    temp_odin_username    = secret_client.get_secret(name="odinprodConnect-username")
    temp_odin_password    = secret_client.get_secret(name="odinprodConnect-password")

    # PYODBC String
    pyodbcodinprod_str = \
        'DRIVER=ODBC Driver 17 for SQL Server;' + \
        'SERVER=' + temp_odin_server + ';' +\
        'DATABASE=odin_prod;' +\
        'UID=' + temp_odin_username.value + ';' +\
        'PWD=' + temp_odin_password.value + ';' + \
        'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

    # PYODBC Object
    try:
        print("Determining status of the database: ")
        pyodbcodinprod_object = pyodbc.connect(pyodbcodinprod_str)
    except:
        print("~Database is sleeping, Starting the Database up!")
        time.sleep(60)
        pyodbcodinprod_object = pyodbc.connect(pyodbcodinprod_str)
    finally:
        print("~Odin Connection is ready!")

    return pyodbcodinprod_str, pyodbcodinprod_object

# Scraping
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
def ScrapeReddit_SubmissionviaSubreddit(Subreddit_Name, conn_reddit_object, MinimumComments=30):
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
                                  submission.id, submission.title, submission.selftext, submission.url, submission.stickied, submission.created_utc,
                                  tempself_CurrentUTC, submission.num_comments, submission.score, submission.upvote_ratio, submission.is_original_content
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
                                          'Submission_ID','Submission_Title','Submission_Selftext','Submission_URL','Submission_Stickied','Submission_UTCCreationTime',
                                          'Submission_UTCFetchTime', 'Submission_NumComments', "Submission_Score",'Submission_UpvoteRatio', 'Submission_OriginalContent'])

    return df

# Insertion Processing
def IP_commentsinfo_commenttracking(Comments_Insertion_Df, Comments_Current_Df, conn_DB_Object):
    # deleteme, conn_DB_Object= connect_to_odinprod(); conn_reddit_object= connect_to_reddit()
    # Comments_Insertion_Df= ScrapeReddit_CommentsviaSubmission("m1ros6", conn_reddit_object)
    # Comments_Current_Df  = CommentsinDB

    NewComments= Comments_Insertion_Df.copy()

    # Removing Current Comments
    NewComments2= NewComments.copy()
    NewComments2["DropMe"]= NewComments["Comment_ID"].isin(Comments_Current_Df["idcomment_reddit"].tolist())

    # Extracting
    temp_idsubmission= pd.read_sql_query("SELECT idsubmission, idsubmission_reddit FROM submission_info "
                                    "WHERE idsubmission_reddit='{}'".format(NewComments.Comment_IDSubmission.unique()[0]),conn_DB_Object)
    temp_idsubreddit = pd.read_sql_query("SELECT TOP(1) idsubmission, idsubreddit FROM submissiontracking "
                                          "WHERE idsubmission='{}'".format(temp_idsubmission["idsubmission"][0]), conn_DB_Object)
    temp_idcomment= pd.read_sql_query("SELECT max(idcomment) as MaxIndex FROM comment_info", conn_DB_Object)

    # Processing into odin format
    NewComments2["idcomment"] = range(temp_idcomment["MaxIndex"][0] + 1, temp_idcomment["MaxIndex"][0] + len(NewComments2) + 1)
    NewComments2["idsubmission"]= temp_idsubmission["idsubmission"][0]
    NewComments2["idsubreddit"]= temp_idsubreddit["idsubreddit"][0]
    NewComments2["initialreplynode"]= NewComments2["Comment_IDParent"]== NewComments2["Comment_IDSubmission"]
    NewComments2["initialreplynode"] = NewComments2["initialreplynode"].astype(int)
    NewComments2["Comment_Stickied"] = NewComments2["Comment_Stickied"].astype(int)

    def tempfunc_datetimetostr(datetime):
        datetime_str = datetime.strftime('%Y-%m-%d %H:%M:%S')
        return datetime_str
    NewComments2["createdatetime"] = NewComments2.apply(lambda row: tempfunc_datetimetostr(row['Comment_UTCCreationTime']), axis=1)
    NewComments2["lastfetched"] = NewComments2.apply(lambda row: tempfunc_datetimetostr(row['Comment_UTCFetchTime']), axis=1)


    # - Processing for parents
    temp_parents = NewComments2[["idcomment", "Comment_ID"]].copy()
    temp_parents.rename(columns={"idcomment": "idparent", "Comment_ID": "idparent_reddit"}, inplace=True)

    NewComments2_Parent = pd.merge(left=NewComments2, right=temp_parents, how="left",
                                   left_on="Comment_IDParent", right_on="idparent_reddit")

    # Finalizing and Returning 2 Files
    NewComments3= NewComments2_Parent[["DropMe","idcomment", "Comment_ID", "idsubmission", "idsubreddit",
                                       "initialreplynode", "Comment_Body",
                                       "idparent", "idparent_reddit", "createdatetime",
                                       "lastfetched", "Comment_score", "Comment_Stickied"]]
    NewComments3.columns= ["DropMe","idcomment", "idcomment_reddit", "idsubmission", "idsubreddit",
                           "initialreplynode", "body",
                           "idparent", "idparent_reddit", "createdatetime",
                           "lastfetched", "score", "stickied"]

    NewComments4= NewComments3[NewComments3.DropMe==False].copy()
    NewComments4.drop(["DropMe"], axis=1, inplace=True)

    # comment_info
    processed_commentinfo= NewComments4[["idcomment", "idcomment_reddit", "idsubmission", "idsubreddit",
                                         "initialreplynode", "body",
                                         "idparent", "idparent_reddit", "createdatetime"]].copy()

    # commenttracking
    processed_commenttracking = NewComments4[["idcomment","lastfetched", "score", "stickied"]].copy()


    return processed_commentinfo, processed_commenttracking
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
    processed_submissioninfo.drop("DropMe", axis=1, inplace=True)

    return processed_submissioninfo

def IP_submissiontracking_reshape(Submission_Raw_Df, conn_Object):
    # deleteme, conn_Object= connect_to_odinprod(); Submission_Raw_Df= Submission_Df

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

    Submission_Raw_Df2["lastfetched"] = datetime.utcfromtimestamp(max(Submission_Raw_Df2["Submission_UTCFetchTime"].astype(int))).\
        strftime('%Y-%m-%d %H:%M:%S')

    # Processing the final submissions
    Submission_Raw_Df3= Submission_Raw_Df2[["idsubmission","idsubreddit",
                                           "lastfetched", "Submission_NumComments", "Submission_Score", "Submission_UpvoteRatio","Submission_Stickied", "isclose"]]
    Submission_Raw_Df3.columns= ["idsubmission","idsubreddit",
                                 "lastfetched", "numcomments", "score", "upvoteratio", "stickied", "isclose"]

    processed_SubmissionTracking= Submission_Raw_Df3

    return processed_SubmissionTracking
def IP_submissiontracking_exclusions(Submission_Raw_Df, conn_Object):
    # deleteme, conn_Object= connect_to_odinprod(); Submission_Raw_Df= Insertme_SubmissionTracking

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
          "\n- {} of {} submissions has already been closed".format(sum(Processing1_Final["isclose"]),len(Processing1_1)) +
          "\n- {} submissions to process ".format(len(Processing1_Final) - sum(Processing1_Final["isclose"]))
          )

    Processing1_Final= Processing1_Final[Processing1_Final["isclose"]==0]

    return Processing1_Final
def IP_submissiontracking_PotentialClosures(Submission_Raw_Df, conn_Object):
    # conn_Object= conn_odin_obj; Submission_Raw_Df= Insertme_SubmissionTracking2


    Processing2_1 = Submission_Raw_Df.copy()
    Processing2_1["DatabaseExistence"]= "New"

    # Extracting Submissions
    print("Processing 2 Started- Modifying Potential Closures IsClosed:")
    Temp_RedditSubmissions= "'"+ "','".join(str(e) for e in Processing2_1["idsubmission"].dropna().astype(int).tolist())+ "'"
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

def IP_subredditinfo(Submission_Raw_Df, conn_Object, conn_RedditObj):
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

logging.info("____"*20)
logging.info("Selflog {}, Admin: Succesfully imported libraries and packages. ".format(datetime.now().strftime("%y/%m/%d %H:%M")))

##############################
# Main
##############################
def main(mytimer: func.TimerRequest) -> None:
    logging.info("____"*20)
    logging.info("Selflog {}, Initializing: Starting Database Insertion Morning Procedure. ".format(datetime.now().strftime("%y/%m/%d %H:%M")))

    # Admin Tweaks and Connections
    logging.info("Admin: Creating respective.")
    Temp_SubredditList = ["stocks", "investing", "wallstreetbets", "Stock_Picks", "pennystocks"]

    conn_reddit_object = connect_to_reddit()
    conn_odin_str, conn_odin_obj = connect_to_odinprod()
    cursor = conn_odin_obj.cursor()
    params = parse.quote_plus(conn_odin_str)
    engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params), fast_executemany=True)    
    logging.info("Selflog {}, Initializing: Completed connections Odin and Reddit. ".format(datetime.now().strftime("%y/%m/%d %H:%M")))

    ########################################################################
    #  Submission Extraction
    ########################################################################
    logging.info("____"*20)
    logging.info("Selflog {}, Scraping Submission: Checking recent reddit submissions.".format(datetime.now().strftime("%y/%m/%d %H:%M")))

    Submission_Df = pd.DataFrame()
    StartTime = datetime.now()

    for Subreddit in Temp_SubredditList:
        # Subreddit= "stocks"
        SubmissionIndiv_Df= ScrapeReddit_SubmissionviaSubreddit(Subreddit_Name=Subreddit, conn_reddit_object=conn_reddit_object, MinimumComments=20)
        Submission_Df = pd.concat([Submission_Df, SubmissionIndiv_Df])

    ########################################################################
    # Database Insertion- LHS
    ########################################################################
    # SubmissionInfo Insertion
    logging.info("____"*20)
    logging.info("Selflog {}, Odin Insertion LHS: Populating tables submission_info.".format(datetime.now().strftime("%y/%m/%d %H:%M")))
    Insertme_SubmissionInfo= IP_submissioninfo(Submission_Df, conn_odin_obj)
    Insertme_SubmissionInfo.to_sql(name="submission_info", con=engine, chunksize=100000, index=False, if_exists="append")
    cursor.commit()

    # Subreddit Insertion
    logging.info("Selflog {}, Odin Insertion LHS: Populating tables subreddit_info.".format(datetime.now().strftime("%y/%m/%d %H:%M")))
    Insertme_SubredditInfo=IP_subredditinfo(Submission_Df, conn_odin_obj, conn_reddit_object)
    Insertme_SubredditInfo.to_sql(name="subreddit_info", con=engine, chunksize=100000, index= False, if_exists="append")
    cursor.commit()

    # Submission Insertion
    logging.info("Selflog {}, Odin Insertion LHS: Populating tables submissiontracking.".format(datetime.now().strftime("%y/%m/%d %H:%M")))
    Insertme_SubmissionTracking = IP_submissiontracking_reshape(Submission_Df, conn_odin_obj)
    Insertme_SubmissionTracking2 = IP_submissiontracking_exclusions(Insertme_SubmissionTracking, conn_odin_obj)
    Insertme_SubmissionTracking3 = IP_submissiontracking_PotentialClosures(Insertme_SubmissionTracking2, conn_odin_obj)
    Insertme_SubmissionTracking3.to_sql(name="submissiontracking", con=engine, chunksize=100000, index=False, if_exists="append")
    cursor.commit()

    SubmissionsToInsert_Raw = Insertme_SubmissionTracking3.copy()

    ####################################
    # RHS Insertion
    ####################################
    logging.info("____"*20)
    logging.info("Selflog {}, Odin Insertion RHS: Communicating with Odin on current comments.".format(datetime.now().strftime("%y/%m/%d %H:%M")))

    # Identify submissions
    temp_script_submissions = ", ".join(str(e) for e in SubmissionsToInsert_Raw["idsubmission"].tolist())
    temp_script1 = "SELECT idsubmission, idsubmission_reddit FROM submission_info WHERE idsubmission in ({})".format(temp_script_submissions)
    SubmissionsToInsert = pd.read_sql_query(temp_script1, conn_odin_obj)

    # Identify current comments
    temp_script2 = "SELECT idcomment, idcomment_reddit, idsubmission FROM comment_info WHERE idsubmission in ({})".format(temp_script_submissions)
    CommentsinDB = pd.read_sql_query(temp_script2, conn_odin_obj)

    # Database Insertion
    logging.info("Selflog {}, Odin Insertion RHS: Initializing comments Insertion.".format(datetime.now().strftime("%y/%m/%d %H:%M")))

    for SubmissionIndex in range(len(SubmissionsToInsert)):
        # SubmissionIndex=0
        print("____" * 10)

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
