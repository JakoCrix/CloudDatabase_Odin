# %% Admin
import pyodbc
from Helper.Connections import connect_to_odinprod

conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()


# %% Subreddit Info Table
#cursor.execute("""DROP TABLE subreddit_info""")
cursor.execute("""CREATE TABLE subreddit_info (
                    idsubreddit INT PRIMARY KEY,
                    idsubreddit_reddit VARCHAR(10) NOT NULL, 
                    createdatetime DATETIME NOT NULL,
                    title VARCHAR(50),
                    url VARCHAR(600) NOT NULL
                    );""")
conn_odin_obj.commit()

# %% Submission Info Table
#cursor.execute("""DROP TABLE submission""")
cursor.execute("""CREATE TABLE submission_info (
                    idsubmission INT PRIMARY KEY,
                    idsubmission_reddit VARCHAR(10) NOT NULL, 
                    title VARCHAR(350) NOT NULL,
                    submissiontext VARCHAR(MAX) NULL,
                    createdatetime DATETIME NOT NULL,
                    url text NOT NULL,
                    originalcontent BIT NOT NULL
                    );""")
conn_odin_obj.commit()

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

