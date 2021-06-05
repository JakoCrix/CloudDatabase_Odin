# %% Admin
import pyodbc
from Helper.Connections_Database import connect_to_odinprod

conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()

# %% SubmissionTracking Table
#cursor.execute("""DROP TABLE submissiontracking""")
cursor.execute("""CREATE TABLE submissiontracking (
                    idsubmission INT FOREIGN KEY REFERENCES submission_info(idsubmission),
                    idsubreddit INT FOREIGN KEY REFERENCES subreddit_info(idsubreddit),
                    lastfetched DATETIME NOT NULL,
                    numcomments INTEGER NOT NULL,
                    score INTEGER NULL,
                    upvoteratio FLOAT NULL,
                    stickied BIT NULL,
                    isclose BIT NOT NULL);
               """)

conn_odin_obj.commit()
conn_odin_obj.close()

