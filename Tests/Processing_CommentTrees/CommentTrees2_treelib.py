# %% Admin
import pandas as pd
from treelib import Node, Tree

# %% Extraction
from Helper.Source import connect_to_db
Conn_Odin= connect_to_db()

# Submission of interest
AllSubmissions= pd.read_sql_query("select * from Submission_Info",Conn_Odin);
AllSubmissions[["ID_Submission", "Title"]].tail(60)
SubmissionOfInterest= "kboh0h"

Temp_Query = "select CI.ID_Comment, CI.ID_ParentID, CI.created_utc from Comment_Information CI " +\
             "where ID_Submission= '{}'".format(SubmissionOfInterest)

AllComments= pd.read_sql_query(Temp_Query,Conn_Odin)
AllComments2= AllComments.copy()
AllComments2["created_utc"]=pd.to_datetime(AllComments2.created_utc)
AllComments2= AllComments2.sort_values("created_utc").reset_index(drop=True)

tree1 = Tree()
tree1.create_node(identifier=SubmissionOfInterest)  # root node
for CommentIndex in range(len(AllComments2)):
    # CommentIndex=0
    ChildID = AllComments2.iloc[CommentIndex]["ID_Comment"]
    ParentID = AllComments2.iloc[CommentIndex]["ID_ParentID"]

    try:
        tree1.create_node(identifier=ChildID, parent=ParentID)
    except:
        print("An exception occurred")

tree1.show()
tree1.paths_to_leaves()
len(tree1.paths_to_leaves())