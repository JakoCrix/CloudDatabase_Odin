# %% Admin
import pyodbc
from Helper.Connections import connect_to_odinprod

conn_odin_str, conn_odin_obj= connect_to_odinprod()
cursor= conn_odin_obj.cursor()


# %% comment Table
#cursor.execute("""DROP TABLE comment""")
cursor.execute("""CREATE TABLE comment_info (
                    idcomment INT PRIMARY KEY,
                    idcomment_reddit VARCHAR(10) NOT NULL, 
                    idsubmission INT FOREIGN KEY REFERENCES submission_info(idsubmission),
                    idsubreddit INT FOREIGN KEY REFERENCES subreddit_info(idsubreddit),
                    initialreplynode BIT NOT NULL,
                    body VARCHAR(MAX) NULL,
                    idparent INT NULL,
                    idparent_reddit VARCHAR(10) NULL,
                    createdatetime DATETIME NOT NULL
                    );
               """)
conn_odin_obj.commit()


# %% commenttracking Table
#cursor.execute("""DROP TABLE commenttracking""")
cursor.execute("""CREATE TABLE commenttracking (
                    idcomment INT FOREIGN KEY REFERENCES comment_info(idcomment),
                    lastfetched DATETIME NOT NULL,
                    score INTEGER NULL,
                    stickied BIT NULL);
               """)
conn_odin_obj.commit()
conn_odin_obj.close()
