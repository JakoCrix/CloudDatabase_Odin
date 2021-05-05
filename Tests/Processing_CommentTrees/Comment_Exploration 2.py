# %% Admin
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from dateutil import tz
import matplotlib.pyplot as plt

from Helper.OdinExtract_Comments import *
from Helper.Source import connect_to_db

# %% Extraction and Processing
# Extraction
Conn_Odin = connect_to_db()
Temp_Query = "select CI.ID_Comment, CI.ID_ParentID, C.body from Comment_Information CI " +\
             "left join Comment C on  CI.ID_Comment=C.ID_Comment " +\
             "where ID_Submission= '{}'".format(SubmissionOfInterest)
AllComments= pd.read_sql_query(Temp_Query,Conn_Odin)


Submission_Raw = ExtractComments_viaSubmission(Conn_Object=Conn_Odin, SubmissionID_str= "kbpinr")
Submission_Raw["SubCreate_US"]= Submission_Raw["CreatedDate"].dt.tz_localize("UTC").dt.tz_convert('America/New_York')
Submission_Raw["ComCreate_US"]= Submission_Raw["created_utc"].dt.tz_localize("UTC").dt.tz_convert('America/New_York')

Temp_SubmissionCreation= Submission_Raw.iloc[0]["SubCreate_US"]
Temp_TotalComments= len(Submission_Raw)
Temp_Closed= Submission_Raw.iloc[Submission_Raw.index[-1]]["IsClosed"]

Submission1= Submission_Raw.drop(["SubCreate_US", "IsClosed","CreatedDate","created_utc"],1).copy()
Submission1.dtypes

# Processing
Submission2= Submission1.copy()
Submission2["ComCreate_US_Ceil"]= Submission2["ComCreate_US"].dt.ceil("15min")
Submission2= Submission2.sort_values("ComCreate_US_Ceil")
Submission2.dtypes

# %% Plot 1
Plot1_Prep= Submission2[["ID_Submission","ComCreate_US_Ceil"]].groupby("ComCreate_US_Ceil").agg({"ID_Submission": "count"}).reset_index()
Plot1_Prep.columns= ["Datetime","Comments"]

Temp_Filling= pd.DataFrame({"Datetime": pd.date_range(min(Plot1_Prep["Datetime"]), max(Plot1_Prep["Datetime"]), freq='15min'),"NumComments_Delete": 0})
Plot1_Prep2= pd.merge(left=Plot1_Prep, right=Temp_Filling, how="outer", on="Datetime").sort_values("Datetime")
Plot1_Prep2["Comments"]= Plot1_Prep2["Comments"].fillna(0)
Plot1_Prep2= Plot1_Prep2.drop("NumComments_Delete", 1)

Plot1_Prep2["Comments_Cumsum"]= (Plot1_Prep2["Comments"].cumsum())/Temp_TotalComments*Plot1_Prep2["Comments"].max()

import matplotlib.dates as mdates
import pytz
plt.plot(Plot1_Prep2["Datetime"], Plot1_Prep2["Comments"])
plt.fill_between(Plot1_Prep2["Datetime"], Plot1_Prep2["Comments_Cumsum"], color="skyblue", alpha=0.4)
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M', tz=pytz.timezone('America/New_York')))
plt.gcf().autofmt_xdate()
plt.xlabel("NY Datetime"); plt.ylabel("#Comments");
plt.show()


