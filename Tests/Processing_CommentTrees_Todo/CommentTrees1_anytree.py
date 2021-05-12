# %% Admin
import pandas as pd
from anytree import Node, RenderTree

# %% Extraction
from Helper.Source import connect_to_db
Conn_Odin= connect_to_db()

# Submission of interest
AllSubmissions= pd.read_sql_query("select * from Submission_Info",Conn_Odin);
AllSubmissions[["ID_Submission", "Title"]].tail(60)
SubmissionOfInterest= "kboh0h"

Temp_Query = "select CI.ID_Comment, CI.ID_ParentID, C.body from Comment_Information CI " +\
             "left join Comment C on  CI.ID_Comment=C.ID_Comment " +\
             "where ID_Submission= '{}'".format(SubmissionOfInterest)

AllComments= pd.read_sql_query(Temp_Query,Conn_Odin)

# %% Cleanup and Processing
from collections import defaultdict
AllComments_Dict = defaultdict(list)
for i in range(len(AllComments)):
    ChildID  = AllComments.iloc[i]["ID_Comment"]
    ParentID = AllComments.iloc[i]["ID_ParentID"]
    AllComments_Dict[ParentID].append(ChildID)
AllComments_Dict.keys()

CommentstoSnip= []
for i in AllComments_Dict[SubmissionOfInterest]:
    if len(AllComments_Dict[i])==0:
        CommentstoSnip.append(i)

#
AllComments2=AllComments.copy()
AllComments_Processed= AllComments2

AllComments2=AllComments[AllComments["ID_Comment"].isin(CommentstoSnip)==False].copy()
AllComments_Processed= AllComments2


# %% Displaying Node
Root= Node("Submission")
for CommentIndex in range(len(AllComments_Processed)):
    # CommentIndex=0
    ChildID  = AllComments_Processed.iloc[CommentIndex]["ID_Comment"]
    ParentID = AllComments_Processed.iloc[CommentIndex]["ID_ParentID"]

    if ParentID==SubmissionOfInterest:
        exec("{} = Node('{}' , parent= Root)".format(ChildID, ChildID))
    else:
        try:
            exec("{} = Node('{}' , parent= {})".format(ChildID, ChildID, ParentID))
        except:
            pass

for pre, fill, node in RenderTree(Root):
    print("%s%s" % (pre, node.name))







