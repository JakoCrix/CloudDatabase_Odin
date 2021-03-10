# Admin
from Helper.Connections import *
import pandas as pd
from datetime import datetime

conn_sqlite_object= connect_to_sqlite()
conn_odin_str, conn_odin_obj= connect_to_odinprod()


# SQL DB Datagrab
temp_query2= """SELECT * FROM Submission_Info"""
SubmissionInfo = pd.read_sql_query(temp_query2, conn_sqlite_object)
SubmissionInfo2 = SubmissionInfo.copy()

# Processing
SubmissionInfo2["idsubmission"]= range(1, len(SubmissionInfo2)+1)
SubmissionInfo2["originalcontent"]= 0

SubmissionInfo3= SubmissionInfo2[["idsubmission", "ID_Submission", "Title", "CreatedDate", "URL", "originalcontent"]]
SubmissionInfo3.columns= ["idsubmission", "idsubmission_reddit", "title", "createdatetime", "url", "originalcontent"]
SubmissionFinal= SubmissionInfo3.copy()

# %% Insertion
cursor= conn_odin_obj.cursor()

insert_sql= "INSERT INTO submission (idsubmission, idsubmission_reddit, title, createdatetime, url, originalcontent) "\
            "VALUES (?, ?, ?, ?, ?, ?)"
records= SubmissionFinal.values.tolist()

cursor.executemany(insert_sql, records)
cursor.commit()

# TODO: we need to truncate Titles before insertion