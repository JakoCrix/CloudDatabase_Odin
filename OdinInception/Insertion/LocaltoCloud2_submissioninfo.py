# Admin
from Helper.Connections_Database import *

import pandas as pd
from sqlalchemy import create_engine
from urllib import parse
from datetime import datetime

# %% SQL DB Extraction
conn_sqlite_object= connect_to_sqlite()
SubmissionInfo_Raw = pd.read_sql_query("""SELECT * FROM Submission_Info""", conn_sqlite_object)
SubmissionInfo1 = SubmissionInfo_Raw.copy()

# %% Processing
SubmissionInfo1["idsubmission"]= range(1, len(SubmissionInfo1)+1)
SubmissionInfo1["originalcontent"]= 0
SubmissionInfo1["submissiontext"]= None

def tempfunc_dtconversion_HMtoHMS(Daterow):
    # Daterow= SubmissionInfo1["CreatedDate"][0]
    DateObj= datetime.strptime(Daterow, "%Y-%m-%d %H:%M")
    return(DateObj.strftime("%Y-%m-%d %H:%M:%S"))
SubmissionInfo1["CreatedDate"]=SubmissionInfo1.apply(lambda row:tempfunc_dtconversion_HMtoHMS(row["CreatedDate"]), axis= 1)

SubmissionInfo2= SubmissionInfo1[["idsubmission", "ID_Submission","Title","submissiontext",
                                  "CreatedDate", "URL", "originalcontent"]]
SubmissionInfo2.columns= ["idsubmission", "idsubmission_reddit","title","submissiontext",
                          "createdatetime", "url", "originalcontent"]
SubmissionFinal=SubmissionInfo2

# %% InsertionProcessing
conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()
params = parse.quote_plus(conn_odin_str)
engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params), fast_executemany=True)

SubmissionFinal.to_sql(name="submission_info", con=engine, chunksize=100000, index= False, if_exists="append")

cursor.commit()
conn_odin_obj.close()