# %% Admin
from datetime import datetime
import pandas as pd
import time
import re

from Odin_Interactions.Extraction.ScrapeComments import ScrapeReddit_CommentsviaSubmission

# Scraping Data



# Function Inserting into odin_prod
def newfunction:
    pass

# Admin
from Helper.Connections import connect_to_odinprod
from Helper.Connections import connect_to_reddit
conn_reddit_object= connect_to_reddit()
conn_odin_str, conn_odin_obj= connect_to_odinprod()

Df_Raw= ScrapeReddit_CommentsviaSubmission("ly92v2", conn_reddit_object)
Df= Df_Raw.copy()

# Current Database
Temp_Submission= Df_Raw.Comment_IDSubmission.unique()
Tempquery= "SELECT CI.idcomment, CI.idcomment_reddit, SI.idsubmission, SI.idsubmission_reddit " \
           "FROM comment_info CI " \
           "LEFT JOIN submission_info SI on CI.idsubmission=SI.idsubmission " \
           "WHERE SI.idsubmission_reddit in ({})".format("'"+"','".join(Temp_Submission)+"'")
Odin_ExistingValues= pd.read_sql_query(Tempquery, conn_odin_obj)


# comment_info
Df= pd.merge(left=Df_Raw, right=idcomment_reddit, how="left", 
             left_on="Comment_ID", right_on="idcomment_reddit")
Df2= Df



# commenttracking







# %%
