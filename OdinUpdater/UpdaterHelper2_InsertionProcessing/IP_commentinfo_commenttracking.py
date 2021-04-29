# %% Admin
from datetime import datetime
import pandas as pd
# from Helper.Connections_Database import *
# from Odin_Interactions.Extraction.ScrapeComments import *

# Function Starts
def IP_commentsinfo_commenttracking(Comments_Insertion_Df, Comments_Current_Df,
                                    conn_DB_Object):
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
